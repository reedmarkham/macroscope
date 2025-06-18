# Cloud Deployment: AWS Migration Strategy

This document outlines the migration path from Docker Compose orchestration to a cloud-native AWS deployment using Kubernetes (EKS), AWS CDK for Infrastructure as Code, and GitHub Actions for CI/CD.

## Architecture Overview

### Current State → Cloud-Native Migration

```
Docker Compose (Local)           →    AWS EKS + CDK (Cloud)
├── Manual orchestration         →    Kubernetes Jobs + Argo Workflows
├── Single-machine limits        →    Auto-scaling across multiple nodes
├── Local storage                →    EFS/S3 with intelligent tiering
└── Manual deployment            →    GitOps with GitHub Actions
```

### AWS Services Integration

| Component | Current | AWS Cloud-Native |
|-----------|---------|------------------|
| **Orchestration** | Docker Compose | Amazon EKS (Kubernetes) |
| **Compute** | Single machine | EC2 Auto Scaling Groups |
| **Storage** | Local volumes | EFS (shared) + S3 (archival) |
| **Networking** | Docker networks | VPC + ALB + Service Mesh |
| **Monitoring** | Local logs | CloudWatch + Prometheus |
| **Infrastructure** | Manual setup | AWS CDK (TypeScript) |
| **CI/CD** | Manual builds | GitHub Actions + ECR |

## Benefits of AWS Migration

### 1. **Scalability & Performance**

**Horizontal Scaling**:
- **Multiple concurrent pipelines**: Process different datasets simultaneously
- **Auto-scaling workers**: Scale OpenOrganelle processing based on dataset size
- **Spot instances**: 70% cost reduction for fault-tolerant workloads

**Resource Optimization**:
```typescript
// CDK: Memory-optimized instance types for heavy workloads
const openOrganelleNodeGroup = new eks.Nodegroup(this, 'OpenOrganelleNodes', {
  instanceTypes: [
    ec2.InstanceType.of(ec2.InstanceClass.R6I, ec2.InstanceSize.XLARGE2), // 64GB RAM
    ec2.InstanceType.of(ec2.InstanceClass.R6I, ec2.InstanceSize.XLARGE4), // 128GB RAM
  ],
  minSize: 0,
  maxSize: 10,
  capacityType: eks.CapacityType.SPOT, // 70% cost savings
});
```

### 2. **Cost Optimization**

**Pay-per-use Model**:
- **Spot instances**: Reduce compute costs by 50-90%
- **Auto-shutdown**: Terminate idle resources automatically
- **S3 Intelligent Tiering**: Automatic cost optimization for long-term storage

**Resource Efficiency**:
```yaml
# Kubernetes: Fine-grained resource allocation
resources:
  requests:
    memory: "4Gi"    # Guaranteed minimum
    cpu: "1000m"
  limits:
    memory: "8Gi"    # Maximum allowed
    cpu: "2000m"
```

### 3. **Reliability & Availability**

**Self-Healing Infrastructure**:
- **Pod restart policies**: Automatic recovery from failures
- **Multi-AZ deployment**: High availability across data centers
- **Health checks**: Proactive monitoring and remediation

**Data Durability**:
- **EFS replication**: Cross-region backup for critical metadata
- **S3 versioning**: Data lineage and recovery capabilities
- **Automated snapshots**: Point-in-time recovery

### 4. **Operational Excellence**

**Monitoring & Observability**:
- **CloudWatch integration**: Centralized logging and metrics
- **X-Ray tracing**: End-to-end pipeline performance analysis
- **Custom dashboards**: Real-time pipeline health monitoring

**Security**:
- **IAM roles**: Fine-grained access control per service
- **VPC isolation**: Network-level security
- **Secrets management**: AWS Secrets Manager integration

## Infrastructure as Code with AWS CDK

### CDK Stack Architecture

```typescript
// lib/em-pipeline-stack.ts
export class EMPipelineStack extends Stack {
  constructor(scope: Construct, id: string, props?: StackProps) {
    super(scope, id, props);

    // 1. VPC with public/private subnets
    const vpc = new ec2.Vpc(this, 'EMPipelineVPC', {
      maxAzs: 3,
      natGateways: 1, // Cost optimization
    });

    // 2. EKS Cluster with optimized node groups
    const cluster = new eks.Cluster(this, 'EMPipelineCluster', {
      vpc,
      version: eks.KubernetesVersion.V1_28,
      defaultCapacity: 0, // We'll add custom node groups
    });

    // 3. Node groups for different workload types
    this.addNodeGroups(cluster);

    // 4. Storage: EFS for shared metadata, S3 for archival
    this.setupStorage(vpc);

    // 5. Monitoring and logging
    this.setupObservability(cluster);
  }

  private addNodeGroups(cluster: eks.Cluster) {
    // Small loaders: Cost-optimized instances
    cluster.addNodegroupCapacity('SmallLoadersNodes', {
      instanceTypes: [ec2.InstanceType.of(ec2.InstanceClass.T3, ec2.InstanceSize.MEDIUM)],
      minSize: 0,
      maxSize: 5,
      capacityType: eks.CapacityType.SPOT,
      taints: [{
        key: 'workload-type',
        value: 'small-loaders',
        effect: eks.TaintEffect.NO_SCHEDULE,
      }],
    });

    // Heavy workloads: Memory-optimized instances
    cluster.addNodegroupCapacity('HeavyWorkloadNodes', {
      instanceTypes: [
        ec2.InstanceType.of(ec2.InstanceClass.R6I, ec2.InstanceSize.XLARGE2),
        ec2.InstanceType.of(ec2.InstanceClass.R6I, ec2.InstanceSize.XLARGE4),
      ],
      minSize: 0,
      maxSize: 3,
      capacityType: eks.CapacityType.ON_DEMAND, // Reliability for critical workloads
      taints: [{
        key: 'workload-type',
        value: 'heavy-processing',
        effect: eks.TaintEffect.NO_SCHEDULE,
      }],
    });
  }

  private setupStorage(vpc: ec2.Vpc) {
    // EFS for shared pipeline metadata
    const fileSystem = new efs.FileSystem(this, 'EMPipelineStorage', {
      vpc,
      performanceMode: efs.PerformanceMode.GENERAL_PURPOSE,
      lifecycle: efs.LifecyclePolicy.AFTER_30_DAYS,
    });

    // S3 bucket with intelligent tiering
    const dataBucket = new s3.Bucket(this, 'EMPipelineData', {
      bucketName: 'em-pipeline-data-processed',
      versioned: true,
      lifecycleRules: [{
        id: 'IntelligentTiering',
        status: s3.BucketLifecycleRuleStatus.ENABLED,
        transitions: [{
          storageClass: s3.StorageClass.INTELLIGENT_TIERING,
          transitionAfter: cdk.Duration.days(1),
        }],
      }],
    });
  }
}
```

### Environment-Specific Deployments

```typescript
// cdk.ts - Multi-environment deployment
const app = new cdk.App();

// Development environment
new EMPipelineStack(app, 'EMPipeline-Dev', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-east-1',
  },
  environmentName: 'dev',
  autoScaling: { min: 0, max: 2 }, // Cost optimization for dev
});

// Production environment
new EMPipelineStack(app, 'EMPipeline-Prod', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: 'us-west-2',
  },
  environmentName: 'prod',
  autoScaling: { min: 1, max: 10 }, // Always-on with high capacity
  multiAZ: true,
});
```

## CI/CD Pipeline with GitHub Actions

### Pipeline Architecture

```yaml
# .github/workflows/deploy-pipeline.yml
name: Deploy EM Pipeline to AWS

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  AWS_REGION: us-west-2
  ECR_REPOSITORY: em-pipeline

jobs:
  # 1. Code Quality & Testing
  quality-gate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Run comprehensive tests
        run: |
          python scripts/run_tests.py unit --coverage
          python scripts/run_tests.py integration --fast
      - name: Code quality checks
        run: scripts/run_lint.sh
      - name: Security scanning
        uses: github/codeql-action/analyze@v3

  # 2. Build and Push Container Images
  build-images:
    needs: quality-gate
    runs-on: ubuntu-latest
    strategy:
      matrix:
        service: [ebi, epfl, flyem, idr, openorganelle, consolidate]
    steps:
      - uses: actions/checkout@v4
      
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Login to Amazon ECR
        id: login-ecr
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push ${{ matrix.service }} image
        env:
          ECR_REGISTRY: ${{ steps.login-ecr.outputs.registry }}
          IMAGE_TAG: ${{ github.sha }}
        run: |
          # Build with buildx for multi-arch support
          docker buildx build \
            --platform linux/amd64,linux/arm64 \
            --push \
            -t $ECR_REGISTRY/$ECR_REPOSITORY-${{ matrix.service }}:$IMAGE_TAG \
            -t $ECR_REGISTRY/$ECR_REPOSITORY-${{ matrix.service }}:latest \
            ./app/${{ matrix.service }}

  # 3. Infrastructure Deployment
  deploy-infrastructure:
    needs: [quality-gate]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      
      - name: Setup Node.js for CDK
        uses: actions/setup-node@v4
        with:
          node-version: '18'
          cache: 'npm'
          cache-dependency-path: 'infrastructure/package-lock.json'

      - name: Install CDK dependencies
        working-directory: infrastructure
        run: npm ci

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Deploy infrastructure
        working-directory: infrastructure
        run: |
          # Deploy based on branch
          if [[ ${{ github.ref }} == "refs/heads/main" ]]; then
            npm run deploy:prod
          else
            npm run deploy:dev
          fi

  # 4. Application Deployment
  deploy-application:
    needs: [build-images, deploy-infrastructure]
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main' || github.ref == 'refs/heads/develop'
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          aws-access-key-id: ${{ secrets.AWS_ACCESS_KEY_ID }}
          aws-secret-access-key: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Update kubeconfig
        run: |
          aws eks update-kubeconfig \
            --region ${{ env.AWS_REGION }} \
            --name EMPipelineCluster-${{ github.ref == 'refs/heads/main' && 'Prod' || 'Dev' }}

      - name: Deploy Kubernetes manifests
        env:
          IMAGE_TAG: ${{ github.sha }}
          ENVIRONMENT: ${{ github.ref == 'refs/heads/main' && 'prod' || 'dev' }}
        run: |
          # Update image tags in Kubernetes manifests
          envsubst < k8s/pipeline-workflow.yaml | kubectl apply -f -
          
          # Wait for deployment to be ready
          kubectl rollout status deployment/em-pipeline-orchestrator

  # 5. Integration Testing in Cloud
  cloud-integration-tests:
    needs: deploy-application
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/develop'
    steps:
      - uses: actions/checkout@v4
      
      - name: Run cloud integration tests
        run: |
          # Test pipeline execution in cloud environment
          python scripts/run_tests.py integration --cloud --environment=dev
          
      - name: Validate data output
        run: |
          # Verify S3 outputs and metadata consistency
          python scripts/validate_cloud_outputs.py --bucket=em-pipeline-data-dev
```

### GitOps Workflow Integration

```yaml
# .github/workflows/gitops-sync.yml
name: GitOps Sync

on:
  push:
    paths: ['k8s/**', 'charts/**']

jobs:
  sync-manifests:
    runs-on: ubuntu-latest
    steps:
      - name: Sync with ArgoCD
        uses: argoproj/argocd-action@v1
        with:
          server: ${{ secrets.ARGOCD_SERVER }}
          token: ${{ secrets.ARGOCD_TOKEN }}
          command: app sync em-pipeline-prod
```

## Kubernetes Workload Definitions

### Workflow-Based Orchestration

```yaml
# k8s/pipeline-workflow.yaml
apiVersion: argoproj.io/v1alpha1
kind: Workflow
metadata:
  name: em-pipeline-workflow
spec:
  entrypoint: em-pipeline-dag
  
  templates:
  - name: em-pipeline-dag
    dag:
      tasks:
      # Phase 1: Start EPFL in background
      - name: epfl-background
        template: epfl-loader
        
      # Phase 2: Sequential small loaders
      - name: small-loaders-sequential
        template: small-loaders
        dependencies: [epfl-background]
        
      # Phase 3: OpenOrganelle with memory optimization
      - name: openorganelle-processing
        template: openorganelle-chunked
        dependencies: [small-loaders-sequential]
        
      # Phase 4: Wait for background processes and consolidate
      - name: metadata-consolidation
        template: consolidate-metadata
        dependencies: [epfl-background, openorganelle-processing]

  - name: epfl-loader
    container:
      image: ${ECR_REGISTRY}/em-pipeline-epfl:${IMAGE_TAG}
      resources:
        requests: { memory: "4Gi", cpu: "1000m" }
        limits: { memory: "6Gi", cpu: "1500m" }
      volumeMounts:
      - name: shared-storage
        mountPath: /app/data
    nodeSelector:
      workload-type: heavy-processing
    tolerations:
    - key: workload-type
      value: heavy-processing
      effect: NoSchedule

  - name: openorganelle-chunked
    container:
      image: ${ECR_REGISTRY}/em-pipeline-openorganelle:${IMAGE_TAG}
      env:
      - name: ZARR_CHUNK_SIZE_MB
        value: "64"
      - name: MAX_WORKERS
        value: "4"
      - name: MEMORY_LIMIT_GB
        value: "8"
      resources:
        requests: { memory: "6Gi", cpu: "2000m" }
        limits: { memory: "8Gi", cpu: "2500m" }
    nodeSelector:
      workload-type: heavy-processing
    tolerations:
    - key: workload-type
      value: heavy-processing
      effect: NoSchedule
```

### Auto-Scaling Configuration

```yaml
# k8s/hpa-openorganelle.yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: openorganelle-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: openorganelle-workers
  minReplicas: 1
  maxReplicas: 5
  metrics:
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 80
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 100
        periodSeconds: 15
    scaleDown:
      stabilizationWindowSeconds: 300
      policies:
      - type: Percent
        value: 10
        periodSeconds: 60
```

## Cost Analysis & Optimization

### Resource Cost Breakdown

| Component | Current (Local) | AWS Cloud (Monthly) | Annual Savings |
|-----------|----------------|---------------------|----------------|
| **Compute** | $0 (own hardware) | $200-800/month¹ | $2,400-9,600 |
| **Storage** | Local disk costs | $50-150/month² | $600-1,800 |
| **Operations** | Manual effort | $0 (automated) | ~$5,000³ |
| **Scaling** | Limited | Pay-per-use | Variable |

¹ *Spot instances can reduce compute costs by 70%*  
² *S3 Intelligent Tiering optimizes storage costs automatically*  
³ *Reduced operational overhead through automation*

### Cost Optimization Strategies

**1. Compute Optimization**:
```typescript
// CDK: Mixed instance types with Spot pricing
const spotMixedInstances = {
  instanceTypes: [
    'r6i.large',    // Primary: 16GB RAM
    'r6i.xlarge',   // Secondary: 32GB RAM  
    'r5.large',     // Fallback: 16GB RAM
  ],
  spotAllocationStrategy: 'diversified',
  spotInstancePools: 3,
};
```

**2. Storage Optimization**:
- **EFS**: Shared metadata with lifecycle management
- **S3 IA**: Infrequent access tier for archival datasets
- **Data compression**: Reduce storage and transfer costs

**3. Network Optimization**:
- **VPC Endpoints**: Avoid NAT gateway charges for S3 access
- **CloudFront**: Cache frequently accessed datasets

## Migration Roadmap

### Phase 1: Foundation (Weeks 1-2)
- [ ] Set up AWS CDK infrastructure stack
- [ ] Create ECR repositories for container images
- [ ] Deploy basic EKS cluster with node groups
- [ ] Migrate container builds to GitHub Actions

### Phase 2: Core Migration (Weeks 3-4)
- [ ] Convert Docker Compose to Kubernetes Jobs
- [ ] Implement basic orchestration with dependencies
- [ ] Set up EFS shared storage and S3 integration
- [ ] Deploy monitoring and logging (CloudWatch)

### Phase 3: Optimization (Weeks 5-6)
- [ ] Implement auto-scaling policies
- [ ] Add Argo Workflows for complex orchestration
- [ ] Optimize resource allocation and cost management
- [ ] Comprehensive integration testing

### Phase 4: Production Readiness (Weeks 7-8)
- [ ] Multi-environment deployment (dev/staging/prod)
- [ ] Disaster recovery and backup strategies
- [ ] Security hardening and compliance
- [ ] Documentation and team training

## Security Considerations

### Infrastructure Security
- **Network isolation**: Private subnets for worker nodes
- **IAM roles**: Least-privilege access per service
- **Secrets management**: AWS Secrets Manager integration
- **Image scanning**: ECR vulnerability scanning

### Data Security
- **Encryption at rest**: EFS and S3 encryption
- **Encryption in transit**: TLS for all communications
- **Access logging**: CloudTrail for audit compliance
- **Data lifecycle**: Automated retention policies

## Monitoring & Observability

### Metrics Collection
```yaml
# CloudWatch custom metrics
metrics:
  - name: pipeline_duration_seconds
    type: histogram
    help: "Total pipeline execution time"
  - name: dataset_processing_errors
    type: counter
    help: "Number of dataset processing failures"
  - name: memory_usage_peak_bytes
    type: gauge
    help: "Peak memory usage per service"
```

### Alerting Strategy
- **Pipeline failures**: Immediate notification to on-call team
- **Resource exhaustion**: Proactive scaling alerts
- **Cost anomalies**: Budget threshold notifications
- **Security events**: Real-time security incident alerts

## Conclusion

Migrating to AWS with EKS provides significant benefits for scaling the electron microscopy data pipeline:

**Key Advantages**:
- **Scalability**: Handle multiple concurrent pipelines
- **Cost efficiency**: Pay-per-use with 70% savings via Spot instances
- **Reliability**: Self-healing infrastructure with multi-AZ availability
- **Operational excellence**: Automated deployment and monitoring

**Migration Effort**: ~8 weeks with proper planning and team training

**ROI Timeline**: Break-even within 6-12 months depending on usage patterns

The combination of Kubernetes orchestration, AWS CDK for infrastructure, and GitHub Actions for CI/CD creates a robust, scalable, and cost-effective solution for production electron microscopy data processing pipelines.