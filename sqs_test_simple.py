import time
import sys
import boto3


def create_queue(sqs, name):
  queue = sqs.create_queue(QueueName=name, Attributes={'DelaySeconds': '5'})
  return queue

def get_queue(sqs, name):
  queue = sqs.get_queue_by_name(QueueName=name)
  return queue

def delete_queue(sqs, name):
  queue = get_queue(sqs, name)
  client = boto3.client('sqs')
  client.delete_queue(QueueUrl=queue.url)

def send_message(queue, message, attributes={}):
  response = queue.send_message(MessageBody=message, MessageAttributes=attributes)
  return response

def generate_message_batch(batch_size, id_start=0):
  assert batch_size <= 10, 'maximum number of entries per request are 10.'
  batch = []
  for i in range(batch_size):
    message = {
        'Id': str(id_start + i),
        'MessageBody': 'ME$$AGE: {}.'.format(id_start + i),
        'MessageAttributes': {'Author': {'StringValue': 'Donald Trump', 'DataType': 'String'}}
    }
    batch.append(message)
  return batch

def generate_message_batches(num_messages):
  assert num_messages % 10 == 0, 'Only supports sending a multiple of 10 messages.'
  batches = []
  num_batches = num_messages // 10
  for i in range(num_batches):
    batch = generate_message_batch(10, id_start=i * 10)
    batches.append(batch)
  return batches

def send_messages(queue, messages, single_batch=True):
  if single_batch:
    response = queue.send_messages(Entries=messages)
    return response
  responses = []
  for batch in messages:
    response = queue.send_messages(Entries=batch)
    responses.append(response)
  return responses

def process_messages(queue, process_fn, attributes=[]):
  x = 0
  for message in queue.receive_messages(
          AttributeNames=['All'],
          MaxNumberOfMessages=10,
          WaitTimeSeconds=10,
          MessageAttributeNames=attributes):
    process_fn(message)
    x += 1
  return x

def sample_message_process_fn(message):
  # Get the custom author message attribute if it was set
  author_text = ''
  if message.message_attributes is not None:
      author_text = message.message_attributes.get('Author').get('StringValue')
  # Print out the body and author (if set)
  print('AUTHOR: {} \t | BODY: {}'.format(author_text, message.body))
  # Let queue know message is processed
  message.delete()


if __name__ == '__main__':
  # python3 file.py QUEUE_NAME
  sqs = boto3.resource('sqs')
  name = sys.argv[1]

  send = False
  if len(sys.argv) > 2:
    send = sys.argv[2]
    send = True if send == 'yes' else False

  print('Creating queue...')
  q = create_queue(sqs, name)
  print(q.url)
  print(q.attributes.get('DelaySeconds'))

  q = get_queue(sqs, name)

  print('\nGot queue...\n', q)

  if send:
    print('\nSending BIG message batch...\n')
    big_message_batch = generate_message_batches(num_messages=200)
    for batch in big_message_batch:
        for message in batch:
            print(message)
    r = send_messages(q, big_message_batch, single_batch=False)
    # print('Response\n\n', r)

  # print('\nProcessing BIG message batch...')
  # process_messages(q, sample_message_process_fn, ['Author'])

  failures = 0
  count = 0
  print('\nProcessing BIG message batch...')
  while True:
    x = process_messages(q, sample_message_process_fn, ['Author'])
    count += x
    if x == 0:
      failures += 1
    if failures > 3:
      break
  print('Finished!', 'Failures', failures, 'Messages processed', count)

