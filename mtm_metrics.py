
from matomodb import MatomoDB
from tqdm import tqdm
import traceback

from mtm_visit import Visit
from mtm_visitor import Visitor
from mtm_action import ActionItem



def main(user, password, host, port, socket, database):
    # Connect to MariaDB
    db = None
    
    
    try:
        db = MatomoDB(database, host, socket, port)
        # breakpoint()
        db.connect(user=user,password=password)

        Visit.init(db)
        Visitor.init(db)
        ActionItem.init(db)
        
        
    except Exception as e:
        # print(f"Error: {e}")
        print(f"{''.join(traceback.format_exception(e))}")
        
    finally:
        if db != None:
            db.close_conn()
    
    


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        '-u', '--user',
        dest='user',
        action='store',
        required=True,
        help='specifies the username to connect with',
    )
    parser.add_argument(
        '-p', '--password',
        dest='password',
        action='store',
        required=False,
        help='specifies the user password',
    )

    parser.add_argument(
        '--host',
        dest='host',
        action='store',
        required=False,
        help='specifies the host to connect to',
    )

    parser.add_argument(
        '--port',
        dest='port',
        action='store',
        default=3306,
        required=False,
        help='specifies the port to connect to',
    )
    
    parser.add_argument(
        '--db',
        dest='db',
        action='store',
        default='matomo',
        required=False,
        help='specifies the database to connect to',
    )

    parser.add_argument(
        '--socket',
        dest='socket',
        action='store',
        default='/tmp/mysql.sock',
        required=False,
        help='specifies the socket to connect to',
    )

    args, unknown = parser.parse_known_args()

    if len(unknown) > 0:
        print(f'Unknown options {unknown}')
        parser.print_help()
        exit(-1)

    main(user=args.user, password=args.password, host=args.host, port=args.port, database=args.db, socket=args.socket)
