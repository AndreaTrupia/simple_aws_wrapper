# simple_aws_wrapper

Example usage:<br>
``
from src.simple_aws_wrapper import config
``

``
from src.simple_aws_wrapper import config
``

``
from src.simple_aws_wrapper.const import regions
``

``
from src.simple_aws_wrapper.const.regions import Region
``

``
from src.simple_aws_wrapper.services.s3 import S3
``

``
aws_config = config.AWSConfig()
``

``
aws_config.set_region(Region(regions.EU_WEST_1))
``

``
s3 = S3()
``

``
message_content=b'Hello World!'
``

``
s3.put_object(message_content, "my-bucket", "my-object-key")
``


