import argparse


def parse_args():
    parser = argparse.ArgumentParser(description='Portfolio API')
    parser.add_argument('-r','--run', type=str, help='What to run')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = parse_args()
    if args.run == 'rec':
        from script.tabulate_reconciliation import tabulate_reconciliation
        tabulate_reconciliation()
    else:
        from portfolio.app import app
        with app.app_context():
            from portfolio.dao import db
            db.create_all()
        app.run(host='0.0.0.0', port=5000, debug=True)
