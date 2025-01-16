
from matomodb import MatomoDB
from tqdm import tqdm
import traceback

from debugout import DebugLevels, debugout, set_dbglevel

from mtm_visit import Visit
from mtm_visitor import Visitor
from mtm_action import ActionItem

class NoRealVisitor(Exception):
    pass

def main(user, password, host, port, socket, database):
    # Connect to MariaDB
    db = None
    
    
    try:
        db = MatomoDB(database, host, socket, port)
        # breakpoint()
        db.connect(user=user,password=password)

        ActionItem.init(db)
        Visit.init(db)
        Visitor.init(db)
        
        nr_testers = 0
        nr_hackers = 0
        nr_visitors = 0

        for vstr_key in Visitor.Visitors:
            visitor = Visitor.Visitors[vstr_key]
            try:
                for vst_key in visitor.visits:
                    visit = visitor.visits[vst_key]
                    if len(visit.actions) == 0:
                        continue
                    all_visits = True
                    at_least_one_visit = False
                    for action in visit.actions:
                        if action.label in ["TESTING","LOCALTESTING"]:
                            raise NoRealVisitor('Tester')
                        elif action.label in ["HACK"]:
                            raise NoRealVisitor('Hacker')
                        elif action.label == None:
                            raise Exception(f'Action {action.idlink_va} in visit {visit.idvisit} has null Label')
                        elif action.label == 'VISIT':
                            at_least_one_visit = True
                        elif action.label != 'VISIT':
                            print(action.label)
                            all_visits = False
                    print(f'All actions are visit: {all_visits}, at least one: {at_least_one_visit}')
                    if not at_least_one_visit:
                        breakpoint()
                    
                nr_visitors += 1
            except NoRealVisitor as e:
                # breakpoint()
                if str(e) == 'Tester':
                    nr_testers += 1
                elif str(e) == 'Hacker':
                    nr_hackers += 1
                
        print(f'visitors {nr_visitors}, testers {nr_testers}, hackers {nr_hackers}')        
    except Exception as e:
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
        '-l','--level',
        dest='level',
        action='store',
        default=1,
        type = int,
        required=False,
        help='specifies the level of output',
    )

    parser.add_argument(
        '--port',
        dest='port',
        action='store',
        default=3306,
        type = int,
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
        debugout(f'Unknown options {unknown}', DebugLevels.ERR)
        parser.print_help()
        exit(-1)

    set_dbglevel(args.level)

    main(user=args.user, password=args.password, host=args.host, port=args.port, database=args.db, socket=args.socket)
