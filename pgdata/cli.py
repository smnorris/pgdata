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
@click.option('--db_url',
              help='Target database Default: $DATABASE_URL',
              envvar='DATABASE_URL')
def cli(dataset, email, db_url):
    """Mirror a DataBC Catalogue dataset in postgres
    """
    db = pgdata.connect(db_url)
    info = db.bcdata2pg(dataset, email)
    click.echo(info['schema']+'.'+info['table'] + ' loaded')


if __name__ == '__main__':
    cli()
