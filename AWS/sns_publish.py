import boto3

"""Example of publishing to SNS

    The topic is linked to the PagerDuty email.
    To Run this update your Okta temporary credentials.
"""
def main():
    client = boto3.client('sns')
    topics = client.list_topics()
    print(topics)
    response = client.publish(
        TopicArn='arn:aws:sns:us-west-1:224919220385:DataEng-PagerDuty-HighUrgency',
        Message='This is a test page',
        Subject='Page from boto3',
    )
    print(response)



if __name__ == '__main__':
    main()
