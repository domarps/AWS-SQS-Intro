import time
import sys
import boto3


def create_queue(sqs, name):
  queue = sqs.create_queue(QueueName=name, Attributes={'DelaySeconds': '5'})
  return queue

def get_queue(sqs, name):
  queue = sqs.get_queue_by_name(QueueName=name)
  return queue

def send_message(queue, message, attributes={}):
  response = queue.send_message(MessageBody=message, MessageAttributes=attributes)
  return response

def generate_message_batch(batch_size, id_start=0):
  assert batch_size <= 10, 'maximum number of entries per request are 10.'
  batch = []
  for i in range(batch_size):
    message = {
        'Id': str(id_start + i),
        'MessageBody': 'hey, this is message: {}.'.format(i),
        'MessageAttributes': {'Author': {'StringValue': 'alex-walczak', 'DataType': 'String'}}
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

def process_messages(queue, process_fn):
  for message in queue.receive_messages(AttributeNames=['All'], MessageAttributeNames=['Author']):
    process_fn(message)

def sample_message_process_fn(message):
  # Get the custom author message attribute if it was set
  author_text = ''
  if message.message_attributes is not None:
      author_text = message.message_attributes.get('Author').get('StringValue')
  # Print out the body and author (if set)
  print('Sup, {}? ------ {}'.format(author_text, message.body))


# FOR FUNSIES:

def _print(message):
  sys.stdout.write('{}\r'.format(message))
  sys.stdout.flush()

def sleeping():
  for i in range(3):
    _print('Sleeping.  ')
    time.sleep(1)
    _print('Sleeping.. ')
    time.sleep(1)
    _print('Sleeping...')
    time.sleep(1)
    _print('Sleeping.. ')
    time.sleep(1)
  print('Sleeping...')


if __name__ == '__main__':
  # python3 file.py QUEUE_NAME
  sqs = boto3.resource('sqs')
  name = sys.argv[1]
  print('Creating queue...')
  q = create_queue(sqs, name)
  print(q.url)
  print(q.attributes.get('DelaySeconds'))

  q = get_queue(sqs, name)
  print('\nGot queue...\n', q)

  print('\nSending BIG message batch...\n', r)
  big_message_batch = generate_message_batches(num_messages=1000)
  r = send_messages(q, big_message_batch, single_batch=False)

  print('\nProcessing BIG message batch...')
  process_messages(q, sample_message_process_fn)

