import logging
import sys
import click
import ckan.plugins as p
import ckan.logic as logic
from ckan.cli import error_shout


log = logging.getLogger(__name__)


@click.group(name=u'jobs', short_help=u"Manage background jobs",
             invoke_without_command=False)
def jobs():
    pass


@jobs.command(name=u'worker',
              short_help=u'Start a worker that fetches jobs '
                         u'from queues and executes them.',
              )
@click.option('--burst', help='Start worker in burst mode.', is_flag=True)
@click.argument(u'queues', metavar=u'QUEUES', nargs=-1, type=click.STRING)
def worker(queues, burst):
    u"""
    Start a worker that fetches jobs from queues and executes
    them. If no queue names are given then the worker listens
    to the default queue, this is equivalent to

        ckan jobs worker default

    If queue names are given then the worker listens to those
    queues and only those:

        ckan jobs worker my-custom-queue

    Hence, if you want the worker to listen to the default queue
    and some others then you must list the default queue explicitly:

        ckan jobs worker default my-custom-queue

    If the `--burst` option is given then the worker will exit
    as soon as all its queues are empty.
    """

    from ckan.lib.jobs import Worker
    Worker(queues=queues).work(burst=burst)


@jobs.command(name=u'list', short_help=u'List currently enqueued jobs from the given queues.')
@click.argument(u'queues', metavar=u'QUEUES', nargs=-1, type=click.STRING)
def list_(queues):
    u"""
    List currently enqueued jobs from the given queues. If no queue
    names are given then the jobs from all queues are listed.
    """

    data_dict = {
        u'queues': queues,
    }
    jobs = p.toolkit.get_action(u'job_list')({}, data_dict)
    for job in jobs:
        if job[u'title'] is None:
            job[u'title'] = ''
        else:
            job[u'title'] = u'"{}"'.format(job[u'title'])
        click.echo(u'{created} {id} {queue} {title}'.format(**job))


@jobs.command(name=u'show', short_help=u'Show details about a specific job.')
@click.argument(u'id', metavar=u'ID',
                type=click.STRING, required=True)
def show(id):
    u"""
    Show details about a specific job.
    """
    # if not self.args:
    #     error(u'You must specify a job ID')
    try:
        job = p.toolkit.get_action(u'job_show')({}, {u'id': id})
    except logic.NotFound:
        error_shout(u'There is no job with ID "{}"'.format(id))
        sys.exit(1)
    click.echo(u'ID:      {}'.format(job[u'id']))
    if job[u'title'] is None:
        title = u'None'
    else:
        title = u'"{}"'.format(job[u'title'])
    click.echo(u'Title:   {}'.format(title))
    click.echo(u'Created: {}'.format(job[u'created']))
    click.echo(u'Queue:   {}'.format(job[u'queue']))


@jobs.command(name=u'cancel', short_help=u'Cancel a specific job.')
@click.argument(u'id', metavar=u'ID', type=click.STRING, required=True)
def cancel(id):
    u"""
    Cancel a specific job. Jobs can only be canceled while they are
    enqueued. Once a worker has started executing a job it cannot
    be aborted anymore.
    """
    try:
        p.toolkit.get_action(u'job_cancel')({}, {u'id': id})
    except logic.NotFound:
        error_shout(u'There is no job with ID "{}"'.format(id))
        sys.exit(1)
    click.echo(u'Cancelled job {}'.format(id))


@jobs.command(name=u'clear', short_help=u'Cancel all jobs on the given queues.')
@click.argument(u'queues', metavar=u'QUEUES', nargs=-1, type=click.STRING)
def clear(queues):
    """
    Cancel all jobs on the given queues.
    If no queue names are given then ALL queues are cleared."""
    data_dict = {
        u'queues': queues,
    }
    queues = p.toolkit.get_action(u'job_clear')({}, data_dict)
    queues = (u'"{}"'.format(q) for q in queues)
    click.echo(u'Cleared queue(s) {}'.format(u', '.join(queues)))


@jobs.command(name=u'test', short_help=u'Enqueue a test job.')
@click.argument(u'queues', metavar=u'QUEUES', nargs=-1, type=click.STRING)
def test(queues):
    """
    Enqueue a test job. If no queue names are given then the job is
    added to the default queue. If queue names are given then a
    separate test job is added to each of the queues.
    """
    from ckan.lib.jobs import DEFAULT_QUEUE_NAME, enqueue, test_job
    for queue in (queues or [DEFAULT_QUEUE_NAME]):
        job = enqueue(test_job, [u'A test job'], title=u'A test job', queue=queue)
        print(u'Added test job {} to queue "{}"'.format(job.id, queue))
