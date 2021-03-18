import pandas as pd
import boto3
import json
import time
import configparser

from botocore.exceptions import ClientError

def create_iam_role(iam, iam_role_name):
    """
    Creates IAM Role by using boto3 client object
    """
    
    stdict = {'Action': 'sts:AssumeRole', 'Effect': 'Allow', 'Principal': {'Service': 'redshift.amazonaws.com'}}
    statement = [stdict]
    try:
        iam.create_role(Path='/', 
                        RoleName=iam_role_name,
                        Description = "Allows Redshift clusters to call AWS services on your behalf.",
                        AssumeRolePolicyDocument=json.dumps({'Statement': statement, 'Version': '2012-10-17'}))
    except Exception as e:
        print(e)
        

        
def attach_policy(iam, iam_role_name):
    """
    Attaches policy to the role by using boto3 client object
    """
    
    iam.attach_role_policy(RoleName=iam_role_name,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
def get_role(iam, iam_role_name): 
    """
    Returns IAM role as Amazon Resource Name (ARN) by using boto3 client object
    """
    
    roleArn = iam.get_role(RoleName=iam_role_name)['Role']['Arn']
    
    return roleArn
        
def create_clients(creds):
    """
    Creates resource and client objects for EC2, S3, IAM and REDSHIFT
    """
    KEY, SECRET = creds
    ec2 = boto3.resource('ec2', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    s3 = boto3.resource('s3', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    iam = boto3.client('iam', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    redshift = boto3.client('redshift', region_name="us-west-2", aws_access_key_id=KEY, aws_secret_access_key=SECRET)
    
    return ec2, s3, iam, redshift


def create_redshift_cluster(redshift, cluster_info, user_pass_dbname, roleArn):
    """
    Creates RedShift Cluster by using specified configurations
    """
    DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER = cluster_info
    DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB = user_pass_dbname
    
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),
            
            #Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,
            
            #Roles (for s3 access)
            IamRoles=[roleArn])
        
    except Exception as e:
        print(e)

def get_endpoint_roleArn(redshift, cluster_identifier):
    """
    Extracts Endpoint and Role Name information from created cluster
    """
    
    props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]    
    DWH_ENDPOINT = props['Endpoint']['Address']
    DWH_ROLE_ARN = props['IamRoles'][0]['IamRoleArn']
    
    return DWH_ENDPOINT, DWH_ROLE_ARN

def open_tcp_ports(ec2, redshift, cluster_identifier, port_number):
    """
    Opens TCP ports of RedShift Cluster for availability from other clients (BI Apps etc.)
    """
    try:
        props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
        vpc = ec2.Vpc(id=props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        
        defaultSg.authorize_ingress(GroupName=defaultSg.group_name,
                                    CidrIp='0.0.0.0/0',
                                    IpProtocol='TCP',
                                    FromPort=int(DWH_PORT),
                                    ToPort=int(DWH_PORT))
    except Exception as e:
        print("WARNING : " + str(e))
        
def open_ports_get_host_roleArn(redshift, ec2, cluster_identifier, cluster_port):
    """
    Checks cluster availability every 30 seconds and waits until the cluster is available.
    Then, opens ports and returns cluster endpoint and role arn.
    """
    s = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]["ClusterStatus"]
    while s != "available":
        s = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]["ClusterStatus"]
        time.sleep(30)
    
    open_tcp_ports(ec2, redshift, cluster_identifier, cluster_port)
    DWH_ENDPOINT, DWH_ROLE_ARN = get_endpoint_roleArn(redshift, DWH_CLUSTER_IDENTIFIER)
    print("\n ----- CONFIG REQUIREMENTS -----\n")
    print("CLUSTER_HOST : \'" + DWH_ENDPOINT + "\'")
    print("ROLE_ARN     : " + DWH_ROLE_ARN)
    print()
    
def show_redshift_props(redshift, cluster_identifier):
    """
    Shows the cluster properties.
    """
    props = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus", 
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes", 'VpcId']
    
    x = [(k, v) for k,v in props.items() if k in keysToShow]
    
    df = pd.DataFrame(data=x, columns=["Key", "Value"])
    
    print("\n ----- REDSHIFT PROPERTIES -----\n")
    print(df)
    print()
    
    return df 

if __name__ == '__main__':
    
    # configure the cluster
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY                    = config.get('AWS','KEY')
    SECRET                 = config.get('AWS','SECRET')

    DWH_CLUSTER_TYPE       = config.get("DWH","DWH_CLUSTER_TYPE")
    DWH_NUM_NODES          = config.get("DWH","DWH_NUM_NODES")
    DWH_NODE_TYPE          = config.get("DWH","DWH_NODE_TYPE")
    DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")

    DWH_DB                 = config.get("DWH","DWH_DB")
    DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT               = config.get("DWH","DWH_PORT")

    DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
    
    # create variables
    credentials = (KEY, SECRET)
    cluster_info = (DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE, DWH_CLUSTER_IDENTIFIER)
    user_pass_dbname = (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)
    
    # create resources and clients 
    ec2, s3, iam, redshift = create_clients(creds=credentials)
    
    # create iam role and attach the policy
    create_iam_role(iam, DWH_IAM_ROLE_NAME)
    attach_policy(iam, DWH_IAM_ROLE_NAME)
    roleArn = get_role(iam, DWH_IAM_ROLE_NAME)
    
    # create redshift cluster
    create_redshift_cluster(redshift, cluster_info, user_pass_dbname, roleArn)
    
    # open ports and get host endpoint and role arn
    open_ports_get_host_roleArn(redshift, ec2, DWH_CLUSTER_IDENTIFIER, DWH_PORT)
    
    # show the properties of the created cluster
    show_redshift_props(redshift, DWH_CLUSTER_IDENTIFIER)