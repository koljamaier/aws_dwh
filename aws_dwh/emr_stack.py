from aws_cdk import (
    aws_iam as iam,
    aws_s3_assets as s3_assets,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfnt,
    aws_glue as glue,
    aws_lambda as lambda_,
    core,
)
import os
from pathlib import Path


def create_emr_instance_role(scope: core.Construct) -> iam.Role:
    """
    create an IAM (service) role for all EC2 instances (the service principal ec2) in our cluster
    the role is assumed by nodes that run insied of EMR/Hadoop and hence they are allowed to talk to other
    AWS services
    under the hood this does pretty much the same as if someone would click the default roles in AWS EMR-UI
    https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
    """
    instance_role = iam.Role(
        scope,
        id=f"EmrInstanceRole",
        assumed_by=iam.ServicePrincipal(f"ec2.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonElasticMapReduceforEC2Role"
            )
        ],
    )

    instance_profile = iam.CfnInstanceProfile(
        scope, "EmrInstanceProfile", roles=[instance_role.role_name]
    )
    return iam.Role.from_role_arn(
        scope, "EmrInstaceProfileRole", instance_profile.attr_arn
    )


def create_emr_service_role(scope: core.Construct) -> iam.Role:
    """
    this role is needed for everything that is not performed within the EMR cluster itself
    e.g. provision the EC2 instances when the EMR cluster spins up
    """
    service_role = iam.Role(
        scope,
        id=f"EmrServiceRole",
        assumed_by=iam.ServicePrincipal(f"elasticmapreduce.amazonaws.com"),
        managed_policies=[
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonElasticMapReduceRole"
            )
        ],
    )

    return service_role


class UdacityCapstoneStack(core.Stack):

    def _create_data_bucket(self):
        """
        This bucket will be the place where our EMR output data is stored. Also the glue crawler will use this
        for inferring our schemas
        """
        bucket_name = f"capstone-uda-data1"
        bucket_id = f"{bucket_name}-bucket"

        bucket = s3.Bucket(
            self,
            id=bucket_id,
            bucket_name=bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
        )

        return bucket

    def _create_emr_logging_bucket(self):
        bucket_name = (
            # make bucket_name 's3-wide-unique' otherwise it cannot be created
            f"emr-logs-udacity-final-project"
        )
        bucket_id = f"{bucket_name}-bucket"

        bucket = s3.Bucket(
            self,
            id=bucket_id,
            bucket_name=bucket_name,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess(
                block_public_acls=True,
                block_public_policy=True,
                ignore_public_acls=True,
                restrict_public_buckets=True,
            ),
        )

        return bucket

    def _create_sfn_pipeline(self):
        pipeline_name = "EMRSparkifyDWH"

        create_cluster_task = self._emr_create_cluster_task(pipeline_name)
        sample_spark_step_task = self._emr_spark_step_task()
        terminate_cluster_task = self._emr_terminate_cluster_task()

        pipeline = (
            create_cluster_task
                .next(sample_spark_step_task)
                .next(terminate_cluster_task)
                .next(self.lambda_glue_crawler_task)
                .next(self.lambda_quality_check_task)
        )

        # Create & deploy StateMachine
        machine = sfn.StateMachine(
            self,
            pipeline_name,
            definition=pipeline,
            role=self.sfn_role,
            state_machine_name=f"{self.stack_name}-{pipeline_name}",
        )

        return machine

    def _emr_create_cluster_task(self, pipeline_name):
        # Let the Stepfunction create a uniform instance group cluster
        # with 1 Master and 5 Core nodes
        create_cluster = sfn.Task(
            self,
            "CreateCluster",
            # this is very similar to the specification menu in AWS UI we used during the course
            task=sfnt.EmrCreateCluster(
                name=pipeline_name,
                applications=[
                    sfnt.EmrCreateCluster.ApplicationConfigProperty(name="spark")
                ],
                # specify the cluster worker/master hardware
                instances=sfnt.EmrCreateCluster.InstancesConfigProperty(
                    instance_groups=[
                        sfnt.EmrCreateCluster.InstanceGroupConfigProperty(
                            instance_count=1,
                            instance_role=sfnt.EmrCreateCluster.InstanceRoleType.MASTER,
                            instance_type="m5.xlarge",
                            name="Master",
                        ),
                        sfnt.EmrCreateCluster.InstanceGroupConfigProperty(
                            instance_count=2,
                            instance_role=sfnt.EmrCreateCluster.InstanceRoleType.CORE,
                            instance_type="m5.xlarge",
                            name="Core",
                        ),
                    ],
                ),
                cluster_role=self.emr_instance_role,
                service_role=self.emr_service_role,
                release_label="emr-6.0.0",
                log_uri=f"s3://{self.emr_logging_bucket.bucket_name}/{pipeline_name}"
            ),
            # we output the ClusterId on the state machine status
            output_path="$.ClusterId",
            result_path="$.ClusterId",
        )
        return create_cluster

    def _emr_spark_step_task(self):
        # Add a EMR Step to run our pyspark job; an asset with our application will be
        # created and referenced in the job definition
        root_path = Path(os.path.dirname(os.path.abspath(__file__)))
        pyspark_script = root_path.joinpath('pyspark', 'example.py').as_posix()
        pyspark_example_asset = s3_assets.Asset(
            self, "PythonScript", path=pyspark_script
        )

        sample_spark_step = sfn.Task(
            self,
            "RunSparkExample",
            task=sfnt.EmrAddStep(
                # the concrete ClusterId will be picked up from the current state of the statem achine
                cluster_id=sfn.Data.string_at("$.ClusterId"),
                name="SparkExample",
                # `command-runner.jar` is a jar from AWS that can be used to execute generic command (like `spark-submit`)
                # if you write your programs in Java/Scala you can directly insert your jar file here instead of script location
                jar="command-runner.jar",
                args=[
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "--master",
                    "yarn",
                    f"s3://{pyspark_example_asset.s3_bucket_name}/{pyspark_example_asset.s3_object_key}",
                ],
            ),
            result_path="DISCARD",
        )
        return sample_spark_step

    def _emr_terminate_cluster_task(self):
        # Shutdown the cluster
        terminate_cluster = sfn.Task(
            self,
            "TerminateCluster",
            task=sfnt.EmrTerminateCluster(
                cluster_id=sfn.Data.string_at("$.ClusterId"),
                integration_pattern=sfn.ServiceIntegrationPattern.SYNC,
            ),
            result_path="DISCARD",
        )
        return terminate_cluster

    def _create_sfn_role(self) -> iam.Role:
        """stepfunction must be authorized to give EMR cluster the corresponding instance/service roles"""
        stack = core.Stack.of(self)

        # allows stepfunction service to pass a role to a different service (in our case EMR) on our behalf
        iam_statements = iam.PolicyStatement(actions=["iam:PassRole"], resources=["*"])

        # allow stepfunction to invoke lambda functions
        invoke_lambda_policy_statement = iam.PolicyStatement(
            actions=["lambda:InvokeFunction"],
            resources=[f"arn:aws:lambda:{stack.region}:*"],
        )

        role = iam.Role(
            self,
            id=f"StepFunctionsServiceRole",
            assumed_by=iam.ServicePrincipal(f"states.{stack.region}.amazonaws.com"),
            inline_policies={
                "sfnAllowPassRole": iam.PolicyDocument(statements=[iam_statements]),
                "sfnAllowInvokeLambda": iam.PolicyDocument(statements=[invoke_lambda_policy_statement]),
            },
            managed_policies=[
                # allows stepfunction to trigger all sorts of EMR actions like DescribeCluster, RunJobFlow,...
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonElasticMapReduceFullAccess"),
                # allows to trigger state machines, tasks,...
                iam.ManagedPolicy.from_aws_managed_policy_name("AWSStepFunctionsFullAccess"),
            ],
        )

        return role

    ### GLUE STUFF
    def _create_glue_db(self):
        """
        Create a glue database that will be visible in Athena
        """
        db_name = self.glue_db_name
        db = glue.Database(
            self,
            f"{db_name}-id",
            database_name=db_name,
            location_uri=f"s3://{self.data_bucket.bucket_name}/",
        )

        return db

    def _create_glue_role(self):
        """
        Setup the permissions our glue crawler needs to have
        """
        return iam.Role(
            self,
            id=f"GlueServiceRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("CloudWatchLogsFullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMReadOnlyAccess"),
            ],
        )

    def _create_glue_crawler(self):
        """
        Implement a glue crawler that can be that runs on our defined data bucket
        :return:
        """
        s3_target = glue.CfnCrawler.S3TargetProperty(
            path=f"s3://{self.data_bucket.bucket_name}/"
        )
        # schedule = "cron(30 5 * * ? *)"

        db_name = self.glue_db_name

        crawler = glue.CfnCrawler(
            self,
            id=f"glue-crawler-{db_name}",
            name=f"{db_name}-crawl",
            database_name=db_name,
            role=self.glue_role.role_arn,
            targets=glue.CfnCrawler.TargetsProperty(s3_targets=[s3_target]),
            # schedule=glue.CfnCrawler.ScheduleProperty(schedule_expression=schedule),
        )

        return crawler

    ### Lambda stuff
    def _lambda_glue_crawler_task(self):
        root_path = Path(os.path.dirname(os.path.abspath(__file__)))
        lambda_handler = root_path.joinpath('lambdas', 'trigger_glue_crawler').as_posix()

        func = lambda_.Function(
            self,
            "TriggerGlueCrawlerLambdaHandler",
            handler="lambda.lambda_handler",
            code=lambda_.AssetCode(
                lambda_handler
            ),
            environment={
                "crawlerName": f"{self.glue_crawler.name}"
            },
            initial_policy=[
                iam.PolicyStatement(
                    actions=["glue:StartCrawler"],
                    resources=["*"],
                ),
            ],
            timeout=core.Duration.seconds(30),
            runtime=lambda_.Runtime.PYTHON_3_7,
        )

        # turn the lambda into a stepfunction task so we can use it in our state machine
        task = sfn.Task(
            self,
            "TriggerGlueCrawlerLambda",
            task=sfnt.InvokeFunction(
                func
            ),
        )

        return task

    def _lambda_quality_check_task(self):
        lambda_role = iam.Role(
            self,
            id=f"QualityLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonS3FullAccess"),
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonAthenaFullAccess"),
            ],
        )

        root_path = Path(os.path.dirname(os.path.abspath(__file__)))
        lambda_handler = root_path.joinpath('lambdas', 'quality_check').as_posix()

        func = lambda_.Function(
            self,
            "QualityCheckAthenaLambdaHandler",
            handler="lambda.lambda_handler",
            code=lambda_.AssetCode(
                lambda_handler
            ),
            environment={
                "athenaDatabase": f"{self.glue_db_name}"
            },
            role=lambda_role,
            timeout=core.Duration.seconds(30),
            runtime=lambda_.Runtime.PYTHON_3_7,
        )

        # turn the lambda into a stepfunction task so we can use it in our state machine
        task = sfn.Task(
            self,
            "QualityCheckAthenaLambda",
            task=sfnt.InvokeFunction(
                func
            ),
        )

        return task

    def __init__(
            self, scope: core.Construct, id: str, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        # EMR setup
        self.sfn_role = self._create_sfn_role().without_policy_updates()
        self.emr_logging_bucket = self._create_emr_logging_bucket()
        self.data_bucket = self._create_data_bucket()
        self.emr_instance_role = create_emr_instance_role(self)
        self.emr_service_role = create_emr_service_role(self)

        # Glue crawler setup
        self.glue_db_name = f"dwh_udacity_capstone"
        self.glue_db = self._create_glue_db()
        self.glue_role = self._create_glue_role()
        self.glue_crawler = self._create_glue_crawler()

        self.lambda_glue_crawler_task = self._lambda_glue_crawler_task()
        self.lambda_quality_check_task = self._lambda_quality_check_task()

        # put together all tasks into a StateMachine/StepFunction etl pipeline
        self.state_machine = self._create_sfn_pipeline()
