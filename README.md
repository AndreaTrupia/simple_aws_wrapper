# simple_aws_wrapper

Example usage:<br>
``
from simple_aws_wrapper.AWS import S3
``

``
s3 = S3("eu-west-1")
``

``
message_content=b'Hello World!'
``

``
s3.put_object(message_content, "my-bucket", "my-object-key")
``


