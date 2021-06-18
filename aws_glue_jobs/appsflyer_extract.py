import sys
from awsglue.utils import getResolvedOptions
import tap_appsflyer


DEGUG = False
args = getResolvedOptions(sys.argv, ['bucket', 'config', 'state'])
bucket = args['bucket']
config_file = args['config']
state_file = args['state']


tap_appsflyer.main(bucket, config_file, state_file)

