import click

import pgdata


def validate_email(ctx, param, value):
    if not value:
        raise click.BadParameter('Provide --email or set $BCDATA_EMAIL')
    else:
        return value


@click.command('bc2pg')
@click.argument('dataset')
@click.option('--email',
              help="Email address. Default: $BCDATA_EMAIL",
              envvar='BCDATA_EMAIL',
              callback=validate_email)
@click.option('--db_url', '-db', help='Database to load files to',
              envvar='FWA_DB')
def cli(dataset, email, db_url):
    """
    Call bcdata2pg function from command line
    """
    db = pgdata.connect(db_url)
    db.bcdata2pg(dataset, email)


if __name__ == '__main__':
    cli()
