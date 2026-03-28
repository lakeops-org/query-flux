import type { FlatClusterForm } from "@/lib/cluster-form/types";
import type { StudioEngineModule } from "@/lib/studio-engines/types";

function validateAthenaClusterFlat(flat: FlatClusterForm): string[] {
  if (!flat.region?.trim()) {
    return ["Athena: AWS region is required."];
  }
  if (!flat.s3OutputLocation?.trim()) {
    return ["Athena: S3 output location is required."];
  }
  if (flat["auth.type"] === "accessKey") {
    if (!flat["auth.username"]?.trim()) {
      return ["Athena: access key ID is required when using static credentials."];
    }
    if (!flat["auth.password"]) {
      return ["Athena: secret access key is required when using static credentials."];
    }
  }
  if (flat["auth.type"] === "roleArn") {
    if (!flat["auth.username"]?.trim()) {
      return ["Athena: role ARN is required when using IAM role assumption."];
    }
  }
  return [];
}

export const athenaStudioEngine: StudioEngineModule = {
  descriptor: {
    engineKey: "athena",
    displayName: "Athena",
    description:
      "Serverless interactive query service by AWS. Queries data in S3 via Glue Data Catalog.",
    hex: "FF9900",
    connectionType: "http",
    defaultPort: null,
    endpointExample: null,
    supportedAuth: ["accessKey", "roleArn"],
    implemented: true,
    configFields: [
      {
        key: "region",
        label: "AWS Region",
        description:
          "AWS region where your Athena workgroup and S3 bucket reside.",
        fieldType: "text",
        required: true,
        example: "us-east-1",
      },
      {
        key: "s3OutputLocation",
        label: "S3 Output Location",
        description:
          "S3 URI where Athena writes query results (e.g. s3://my-bucket/athena-results/).",
        fieldType: "text",
        required: true,
        example: "s3://my-bucket/athena-results/",
      },
      {
        key: "workgroup",
        label: "Workgroup",
        description: "Athena workgroup to run queries in. Defaults to 'primary'.",
        fieldType: "text",
        required: false,
        example: "primary",
      },
      {
        key: "catalog",
        label: "Catalog",
        description: "Default Glue catalog. Defaults to 'AwsDataCatalog'.",
        fieldType: "text",
        required: false,
        example: "AwsDataCatalog",
      },
      {
        key: "auth.type",
        label: "AWS Credentials",
        description:
          "Choose 'accessKey' for static IAM credentials, or leave unset to use the default AWS credential chain (env vars, ECS role, EC2 instance profile).",
        fieldType: "text",
        required: false,
      },
      {
        key: "auth.username",
        label: "Access Key ID",
        description: "IAM access key ID (required when auth = accessKey).",
        fieldType: "text",
        required: false,
        example: "AKIAIOSFODNN7EXAMPLE",
      },
      {
        key: "auth.password",
        label: "Secret Access Key",
        description: "IAM secret access key.",
        fieldType: "secret",
        required: false,
      },
    ],
  },
  catalog: {
    category: "Cloud Warehouse",
    simpleIconSlug: null,
    catalogDescription: "Serverless interactive query service by AWS",
  },
  validateFlat: validateAthenaClusterFlat,
  customFormId: "athena",
  engineAffinity: false,
};
