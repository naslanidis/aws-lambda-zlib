from __future__ import print_function

import urllib
import boto3
import zlib
import os

print('Loading function')
s3 = boto3.client('s3')


def lambda_handler(event, context):

    ZLIB_MARKERS = ['\x78\x9c']

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.unquote_plus(event['Records'][0]['s3']['object']['key'].encode('utf8'))
    s3_path = os.path.dirname(key)
 
    filename = key

    try:
        filename = key + '.compressed'
        s3.download_file(bucket, key, '/tmp/' + key + '.compressed')
        infile = open('/tmp/' + filename, 'r')
        data = infile.read()

        pos = 0
        found = False

        while not found:
            window = data[pos:pos+2]
            for marker in ZLIB_MARKERS:
                if window == marker:
                    found = True
                    start = pos
                    break
            if pos == len(data):
                break
            pos += 1

        if found:
            header = data[:start]

            rest_of_data = data[start:]
            decomp_obj = zlib.decompressobj()
            uncompressed_msg = decomp_obj.decompress(rest_of_data)

            print ("Message: %s" % uncompressed_msg)
            localpath = '/tmp/' + key + '.decompressed'

            outfile = open(localpath, 'wb')
            outfile.write(uncompressed_msg)
            outfile.close()

            s3.upload_file(localpath, bucket, os.path.join(s3_path, key + '.decompressed'))
            s3.delete_object(Bucket=bucket, Key=key)

        return key
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
