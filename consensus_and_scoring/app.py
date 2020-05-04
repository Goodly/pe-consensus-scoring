import json

import master

def lambda_handler(event, context):
    """
    Parameters
    ----------
    event: dict, required
        SQS Receive Message

    context: object, required
        Lambda Context runtime methods and attributes
        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html

    Returns
    ------
    Sends to output queue.

    """

    print("Log something.")

    return {'done': True}
