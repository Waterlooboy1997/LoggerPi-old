"""Small, truthful summaries for the compact site list."""
from pathlib import Path
import sqlite3

FAILURES={'failed','limited','interrupted'}


def short_error(value):
    text=str(value or '').strip()
    lower=text.lower()
    if 'connection failed after 20 seconds' in lower or 'no pakbus response within 20 seconds' in lower:return 'Connection failed after 20 seconds'
    if 'serial' in lower and ('mismatch' in lower or 'expected' in lower):return 'Logger serial does not match'
    if 'array field' in lower:return 'Unsupported array field'
    if 'recdata' in lower:return 'No response from logger'
    if any(s in lower for s in ('timed out','timeout','deadline','budget')):return 'Attempt timed out'
    if any(s in lower for s in ('getaddrinfo','name resolution','no such host')):return 'Address could not be found'
    if 'refused' in lower:return 'Connection refused'
    if any(s in lower for s in ('reset by peer','connection reset','connection aborted')):return 'Connection dropped'
    if 'connection or collection failed' in lower:return 'Collection failed — see log'
    return text.splitlines()[0][:140] if text else 'Collection failed — see log'


def last_records(output_directory):
    """Read actual committed rows, never a query coverage/checkpoint timestamp."""
    path=Path(output_directory)/'collector.sqlite3'
    if not path.exists():return {}
    result={}
    try:
        db=sqlite3.connect(path.resolve().as_uri()+'?mode=ro',uri=True,timeout=2)
        try:
            generations=db.execute('SELECT id,site,tab FROM generations').fetchall()
            for identifier,site,table in generations:
                row=db.execute('SELECT stamp,rec FROM records WHERE gen=? ORDER BY stamp DESC,rec DESC LIMIT 1',(identifier,)).fetchone()
                key=(site,table)
                if row and (key not in result or row[0]>result[key]['timestamp']):
                    result[key]={'timestamp':row[0],'record':row[1]}
        finally:db.close()
    except sqlite3.Error:
        # UI remains available if the data journal is temporarily unavailable.
        return {}
    return result


def summarize(site,latest,tables,active,config_issues,retrying,records,last_success):
    primary='Measurement' if 'Measurement' in site['tables'] else site['tables'][0]
    failed=[t for t in tables if t['state'] in FAILURES]
    succeeded=[t for t in tables if t['state']=='success']
    detail=config_issues[0] if config_issues else ''
    status='Scheduled'
    if active:
        job=next((j for j in active if j['state']=='running'),active[0])
        if job['state']=='running':status=job.get('phase') or 'Starting'
        else:status='Waiting for connection'
    elif config_issues:status='Needs configuration'
    elif latest and latest['state'] in FAILURES and latest['kind']=='test':status='Failed'
    elif not site['enabled']:status='Disabled'
    elif failed and succeeded:status='Partial failure'
    elif failed or (latest and latest['state'] in FAILURES):
        status='Retrying' if retrying else 'Failed'
    elif any(t['state']=='success' for t in tables) or (latest and latest['state']=='success'):status='OK'
    if latest and latest['state'] in FAILURES and not latest.get('automatic'):detail=short_error(latest.get('message'))
    elif failed:detail=short_error(failed[0].get('message'))
    elif latest and latest['state'] in FAILURES:detail=short_error(latest.get('message'))
    elif latest and latest['kind']=='test' and latest['state']=='success':detail='Connection check passed'
    attention=bool(config_issues or failed or (latest and latest['state'] in FAILURES))
    record=records.get((site['name'],primary),{})
    return {'id':site['id'],'primary_table':primary,'status':status,'detail':detail,
            'needs_attention':attention,'last_success':last_success,
            'last_record':record.get('timestamp'),'record_number':record.get('record'),
            'tables':tables,'active_jobs':active}
