from aws_cdk import (
    aws_iam as iam,
    aws_sqs as sqs,
    aws_sns as sns,
    aws_sns_subscriptions as subs,
    aws_emr as emr,
    aws_ec2 as ec2,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfnt,
    core
)


from aws_cdk import (
    aws_s3_assets as s3_assets,
    aws_s3 as s3,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfnt,
    aws_glue as glue,
    core,
)
from aws_cdk.core import CfnTag

import os
from pathlib import Path


def create_emr_instance_role(scope: core.Construct) -> iam.Role:
    # create an IAM (service) role for all EC2 instances (the service principal ec2) in our cluster
    # the role is assumed by nodes that run insied of EMR/Hadoop and hence they are allowed to talk to other
    # AWS services
    # under the hood this does pretty much the same as if someone would click the default roles in AWS EMR-UI
    # https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-iam-role-for-ec2.html
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



# this role is needed for everything that is not performed within the EMR cluster itself
# e.g. provision the EC2 instances when the EMR cluster spins up
def create_emr_service_role(scope: core.Construct) -> iam.Role:
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

class EmrTestStack(core.Stack):


    def _create_data_bucket(self):
        """
        This bucket will be the place where our EMR output data is stored. Also the glue crawler will use this
        for inferring our schemas
        """
        bucket_name = f"capstone-uda-data"
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
        pipeline_name = "EmrTest"

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


        pipeline = (
            create_cluster
                .next(sample_spark_step)
                # we can uncomment the following in the future
                #.next(terminate_cluster)
        )

        # Create & deploy StateMachine
        machine = sfn.StateMachine(
            self,
            pipeline_name,
            definition=pipeline,
            role=self.sfn_role,
            state_machine_name=f"{self.stack_name}-{pipeline_name}",
        )

    def _create_sfn_role(self) -> iam.Role:
        """"stepfunction must be authorized to give EMR cluster the corresponding instance/service roles"""
        stack = core.Stack.of(self)

        iam_statements = iam.PolicyStatement(actions=["iam:PassRole"], resources=["*"])

        emr_policy_statements = [
            iam.PolicyStatement(
                actions=[
                    "elasticmapreduce:RunJobFlow",
                    "elasticmapreduce:DescribeCluster",
                    "elasticmapreduce:TerminateJobFlows",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "elasticmapreduce:AddJobFlowSteps",
                    "elasticmapreduce:DescribeStep",
                    "elasticmapreduce:CancelSteps",
                    "elasticmapreduce:SetTerminationProtection",
                    "elasticmapreduce:ModifyInstanceFleet",
                    "elasticmapreduce:ListInstanceFleets",
                    "elasticmapreduce:ModifyInstanceGroups",
                    "elasticmapreduce:ListInstanceGroups",
                ],
                resources=["arn:aws:elasticmapreduce:*:*:cluster/*"],
            ),
            iam.PolicyStatement(
                actions=["iam:CreateServiceLinkedRole", "iam:PutRolePolicy"],
                resources=[
                    "arn:aws:iam::*:role/aws-service-role/elasticmapreduce.amazonaws.com*/AWSServiceRoleForEMRCleanup*"
                ],
                conditions={
                    "StringLike": {"iam:AWSServiceName": ["elasticmapreduce.amazonaws.com"]}
                },
            ),
        ]

        sfn_policy_statements = [
            iam.PolicyStatement(
                actions=["states:DescribeExecution", "states:StopExecution"],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=["states:StartExecution"],
                resources=[f"arn:aws:states:{stack.region}:{stack.account}:stateMachine:*"],
            ),
            iam.PolicyStatement(
                actions=["events:PutTargets", "events:PutRule", "events:DescribeRule"],
                resources=[
                    f"arn:aws:events:{stack.region}:"
                    f"{stack.account}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"
                ],
            ),
        ]

        role = iam.Role(
            self,
            id=f"StepFunctionsServiceRole",
            assumed_by=iam.ServicePrincipal(f"states.{stack.region}.amazonaws.com"),
            inline_policies={
                "sfnAllowPassRole": iam.PolicyDocument(statements=[iam_statements]),
                "sfnAllowRunSfn": iam.PolicyDocument(statements=sfn_policy_statements),
                "sfnAllowRunEMR": iam.PolicyDocument(statements=emr_policy_statements),
            },
        )

        return role



    ### GLUE STUFF
    ### davor noch eine glue DB erzeugen die dann in Athena sichtbar ist
    def _create_glue_db(self):
        # db_name = f"{get_environment_name(stack)}_{zone.db_name}"
        db_name = self.glue_db_name
        db = glue.Database(
            self,
            f"{db_name}-id",
            database_name=db_name,
            location_uri=f"s3://{self.data_bucket.bucket_name}/",
        )

        return db

    # Die rolle sollte man easy erstellen können mittels
    def _create_glue_role(self):
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
        s3_target = glue.CfnCrawler.S3TargetProperty(
            # TODO exclusions auskommentieren
            path=f"s3://{self.data_bucket.bucket_name}/", exclusions=["**"]
        )
        # schedule = "cron(30 5 * * ? *)"

        db_name = self.glue_db_name

        # der crawler wird mitsamt triggern initialisiert. Deshalb muss also kein extra Schritt in der StepFunctions definiert werden
        # (es gibt sowieso keinen Stepfunction Task der das ausführen könnte
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



    def __init__(
        self, scope: core.Construct, id: str, **kwargs
    ) -> None:
        super().__init__(scope, id, **kwargs)

        self.sfn_role = self._create_sfn_role().without_policy_updates()
        self.emr_logging_bucket = self._create_emr_logging_bucket()
        self.data_bucket = self._create_data_bucket()

        self.emr_instance_role = create_emr_instance_role(self)
        self.emr_service_role = create_emr_service_role(self)

        self.glue_db_name = f"dwh_udacity_capstone"
        self.glue_db = self._create_glue_db()
        self.glue_role = self._create_glue_role()
        self.glue_crawler = self._create_glue_crawler()

        # TODO create lambda & policy anfügen AWSGlueConsoleFullAccess, sodass sie den Trigger starten darf

        self._create_sfn_pipeline()
