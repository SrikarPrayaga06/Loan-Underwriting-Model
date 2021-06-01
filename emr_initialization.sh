# TeamSJ -- EMR setup details


# Configured emr-6.2.0 and encompassed Hadoop 3.2.1, Hive 3.1.2, Hue 4.8.0, Spark 3.0.1, and Pig 0.17.0

# Assigned one master node and two worker nodes for the components of EMR cluster

# Compisition at Uniform instance groups, left Network at default VPC and EC2 subnet suggested by AWS

# Disabled cluster scaling and set EBS volume size at 10GiB for our root device

# Named our cluster as Teamsj-cluster and logged our S3 bucket(dsc102-teamsj-scratch) on it with the same key pair we used for EC2 instantiation

# Left permission for our cluster at default setting 

# Once the cluster was initiated, we modified the Security Group ID in Elasticmapreduce-master node with additional inbound rules to have SSH source from our local IP address. 