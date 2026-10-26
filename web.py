"""LAN administration UI. Run separately from scheduler.py."""
import argparse
import copy
import hashlib
import json
import os
from pathlib import Path
import secrets
import time
from flask import Flask, jsonify, redirect, render_template, request, session
from werkzeug.security import generate_password_hash, check_password_hash
from control import Control, defaults, issues, redact, backup_preview


def create_app(state='state'):
    control=Control(state)
    app=Flask(__name__)
    app.secret_key=control.get('session_secret')
    app.config.update(SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SAMESITE='Strict',MAX_CONTENT_LENGTH=20*1024*1024,PERMANENT_SESSION_LIFETIME=28800)
    app.extensions['control']=control
    if not control.get('password_hash') and not control.get('setup_hash'):
        token=secrets.token_urlsafe(24)
        control.set('setup_hash',hashlib.sha256(token.encode()).hexdigest())
        path=control.root/'setup-code.txt'
        fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_TRUNC,0o600)
        with os.fdopen(fd,'w') as handle:handle.write(token+'\n')
    attempts={}

    def csrf():
        if 'csrf' not in session:session['csrf']=secrets.token_urlsafe(24)
        return session['csrf']

    @app.before_request
    def protect():
        public=request.endpoint in ('static','login','setup')
        if not public and not session.get('authenticated'):
            if request.path.startswith('/api/'):
                return jsonify(error='Login required'),401
            return redirect('/login' if control.get('password_hash') else '/setup')
        if request.method in ('POST','PUT','DELETE','PATCH'):
            token=request.headers.get('X-CSRF-Token') or request.form.get('csrf')
            if not token or not secrets.compare_digest(token,session.get('csrf','')):
                return jsonify(error='Invalid form token. Reload the page.'),403
        if request.endpoint in ('login','setup') and request.method=='POST':
            now=time.time();key=request.remote_addr
            attempts[key]=[t for t in attempts.get(key,[]) if now-t<60]
            if len(attempts[key])>=5:return jsonify(error='Too many attempts. Wait one minute.'),429
            attempts[key].append(now)

    @app.after_request
    def headers(response):
        response.headers['X-Content-Type-Options']='nosniff'
        response.headers['X-Frame-Options']='DENY'
        response.headers['Cache-Control']='no-store'
        response.headers['Referrer-Policy']='no-referrer'
        response.headers['Content-Security-Policy']="default-src 'self'; style-src 'self'; script-src 'self'; frame-ancestors 'none'; base-uri 'self'; form-action 'self'"
        return response

    @app.errorhandler(ValueError)
    def bad_request(error):return jsonify(error=str(error)),400

    @app.errorhandler(413)
    def too_large(error):return jsonify(error='Upload too large; maximum 20 MB.'),413

    @app.route('/setup',methods=['GET','POST'])
    def setup():
        if control.get('password_hash'):return redirect('/login')
        if request.method=='POST':
            password=request.form.get('password','')
            token=request.form.get('token','')
            if len(password)<12:raise ValueError('Use a password with at least 12 characters.')
            if not secrets.compare_digest(hashlib.sha256(token.encode()).hexdigest(),control.get('setup_hash','')):
                raise ValueError('Incorrect setup code.')
            control.set('password_hash',generate_password_hash(password))
            control.set('setup_hash',None)
            (control.root/'setup-code.txt').unlink(missing_ok=True)
            session.clear();session['authenticated']=True;session.permanent=True
            return redirect('/')
        return render_template('auth.html',setup=True,csrf=csrf(),code_path=str(control.root/'setup-code.txt'))

    @app.route('/login',methods=['GET','POST'])
    def login():
        if not control.get('password_hash'):return redirect('/setup')
        error=None
        if request.method=='POST':
            if check_password_hash(control.get('password_hash'),request.form.get('password','')):
                session.clear();session['authenticated']=True;session.permanent=True
                return redirect('/')
            error='Incorrect password.'
        return render_template('auth.html',setup=False,csrf=csrf(),error=error),401 if error else 200

    @app.post('/logout')
    def logout():session.clear();return redirect('/login')

    @app.get('/')
    def index():return render_template('index.html',csrf=csrf())

    @app.get('/api/status')
    def status():return jsonify(control.status())

    @app.get('/api/config/export')
    def export():
        response=jsonify(redact(control.config()))
        response.headers['Content-Disposition']='attachment; filename=collector-settings.json'
        return response

    @app.put('/api/config')
    def save():
        payload=request.get_json()
        previous=control.config();by_id={s['id']:s for s in previous['sites']}
        sites=[]
        for incoming in payload['sites']:
            incoming=copy.deepcopy(incoming)
            old=by_id.get(incoming.get('id'),{})
            if 'security_code' not in incoming and 'security_code' in old:incoming['security_code']=old['security_code']
            incoming.pop('has_security_code',None)
            sites.append(defaults(incoming))
        updated=control.save_config(dict(previous,sites=sites,max_connections=payload.get('max_connections',previous.get('max_connections',4)),output_directory=payload.get('output_directory',previous['output_directory'])),payload['revision'])
        return jsonify(redact(updated))

    @app.post('/api/import/backup')
    def preview():
        upload=request.files.get('file')
        if upload is None:raise ValueError('Choose a LoggerNet .bkp file.')
        return jsonify(sites=backup_preview(upload.read(),Path(__file__).parent/'reference_data'))

    @app.post('/api/sites/<identifier>/jobs')
    def enqueue(identifier):
        site=next((s for s in control.config()['sites'] if s['id']==identifier),None)
        if site is None:raise ValueError('Unknown site.')
        data=request.get_json();kind=data.get('kind');override=data.get('override_window',False)
        if not isinstance(override,bool):raise ValueError('Window override must be true or false.')
        ids=[control.enqueue(site,kind,None,override)] if kind=='test' else [control.enqueue(site,kind,t,override) for t in site['tables']]
        return jsonify(jobs=ids),202

    @app.post('/api/sites')
    def add_site():
        data=request.get_json()
        site=copy.deepcopy(data.get('site') or {});site.pop('has_security_code',None)
        return jsonify(redact(control.add_site(site,data.get('confirm')))),201

    @app.post('/api/sites/<identifier>/remove')
    def remove_site(identifier):
        data=request.get_json()
        return jsonify(redact(control.remove_site(identifier,data.get('confirm'),data.get('revision'))))

    @app.post('/api/sites/<identifier>/stop-history')
    def stop_history(identifier):
        with control.connect() as db:
            db.execute("UPDATE jobs SET cancel=1,state=CASE WHEN state='queued' THEN 'cancelled' ELSE state END,message='History download stopped' WHERE site=? AND kind='backfill' AND state IN ('queued','running')",(identifier,))
        return jsonify(ok=True)

    @app.post('/api/jobs/<int:identifier>/cancel')
    def cancel(identifier):
        with control.connect() as db:
            db.execute("UPDATE jobs SET cancel=1,state=CASE WHEN state='queued' THEN 'cancelled' ELSE state END,message='Cancellation requested' WHERE id=? AND state IN ('queued','running')",(identifier,))
        return jsonify(ok=True)

    @app.get('/api/jobs/<int:identifier>/log')
    def log(identifier):
        path=control.root/'logs'/f'job-{identifier}.log'
        if not path.exists():return jsonify(log='No log yet.')
        with path.open('rb') as handle:
            handle.seek(max(0,path.stat().st_size-20000));value=handle.read().decode('utf-8',errors='replace')
        # Do not return raw protocol payloads or configuration snapshots.
        return jsonify(log=value)

    @app.post('/api/revisions/<int:identifier>/restore')
    def restore(identifier):
        with control.connect() as db:
            row=db.execute('SELECT config FROM revisions WHERE id=?',(identifier,)).fetchone()
        if row is None:raise ValueError('Unknown revision.')
        saved=json.loads(row[0])
        expected=request.get_json().get('revision')
        return jsonify(redact(control.save_config(saved,expected,membership=True)))

    @app.get('/api/revisions')
    def revisions():
        with control.connect() as db:
            rows=[{'revision':r['id'],'stamp':r['stamp'],'config':redact(json.loads(r['config']))} for r in db.execute('SELECT * FROM revisions ORDER BY id DESC LIMIT 25')]
        return jsonify(revisions=rows)

    return app


def main():
    p=argparse.ArgumentParser();p.add_argument('--state',default='state');p.add_argument('--host',default='0.0.0.0');p.add_argument('--port',default=8889,type=int)
    p.add_argument('--reset-password',action='store_true');p.add_argument('--import-settings')
    args=p.parse_args();control=Control(args.state)
    if args.reset_password:
        control.set('password_hash','');control.set('setup_hash',None)
        control.set('session_secret',secrets.token_hex(32))
    if args.import_settings:
        if control.config()['revision']:
            p.error('Settings already exist; use the webpage to edit.')
        original=json.loads(Path(args.import_settings).read_text(encoding='utf-8-sig'))
        conf=control.config();conf['sites']=[defaults(dict(s,enabled=False)) for s in original['sites']]
        control.save_config(conf,0,membership=True)
    app=create_app(args.state)
    print(f'Collector administration: http://localhost:{args.port}',flush=True)
    if not control.get('password_hash'):print(f'Initial setup code: {control.root / "setup-code.txt"}',flush=True)
    from waitress import serve
    serve(app,host=args.host,port=args.port,threads=4)


if __name__=='__main__':main()
