'use strict';
const _ = require('lodash');
const dns = require('dns');
const events = require('events');
const request = require('request');
const hutil = require('hutil');
const etask = hutil.etask;
const qw = hutil.string.qw;
const assign = Object.assign;
const E = exports;

E.defaults = {
    customer: process.env.LUMINATI_CUSTOMER,
    password: process.env.LUMINATI_PASSWORD,
    concurrency: 10,
    retries: 3,
    proxy: 'zproxy.luminati.io',
    port: 22225,
    zone: 'gen',
    request: {timeout: 5000},
};
E.batch = (urls, opt)=>{
    opt = assign({}, E.defaults, opt||{});
    let requests = {};
    const handler = new events();
    handler.abort = ()=>{
        urls = null;
        for (let key in requests)
        {
            requests[key].req.abort();
            requests[key].etask.return();
        }
        requests = {};
    };    
    etask(function*batch(){
        this.on('ensure', ()=>{
            if (this.error)
                handler.on('error', this.error);
        });
        if (typeof opt.proxy=='number')
        {
            opt.proxy = Math.min(opt.proxy, opt.concurrency, urls.length);
            const hosts = {};
            const start = Date.now();
            while (Object.keys(hosts).length<opt.proxy &&
                Date.now()-start<30000)
            {
                let ips = yield etask.nfn_apply(dns, '.resolve',
                    [`session-${Date.now()}.zproxy.luminati.io`]);
                ips.forEach(ip=>hosts[ip] = true);
            }
            opt.proxy = Object.keys(hosts);
        }
        else if (typeof opt.proxy=='string')
        {
            if (/^\d+\.\d+\.\d+\.\d+$/.test(opt.proxy))
                opt.proxy = [opt.proxy];
            else
            {
                opt.proxy = yield etask.nfn_apply(dns, '.resolve',
                    [opt.proxy]);
            }
        }
        const res = [].concat(urls);
        let concurrency = Math.min(opt.concurrency, urls.length);
        const queue = [];
        while (concurrency-->0)
        {
            let proxy = opt.proxy.shift();
            opt.proxy.push(proxy);
            const auth = assign({session: `${Date.now()}_${concurrency}`},
                _.pick(opt, qw`zone session country state city asn dns`));
            let username = 'lum-customer-'+opt.customer;
            for (let key in auth)
                username += '-'+key+'-'+auth[key];
            proxy = `http://${username}:${opt.password}@${proxy}:${opt.port}`;
            const requests = opt.requests||[];
            queue.push(etask(function*task(){
                while (urls && urls.length)
                {
                    let i = res.length-urls.length;
                    const url = urls.shift();
                    const req_opt = requests.shift()||opt.request;
                    for (let attempt=1; urls; attempt++)
                    {
                        requests[i] = {etask: this};
                        const e_opt = {ret_sync: [requests[i], 'req']};
                        try {
                            res[i] = yield etask.nfn_apply(e_opt, request,
                                [assign({url: url, proxy: proxy}, req_opt)]);
                            handler.emit('success', res[i], {url: url,
                                proxy: proxy, index: i, attempt: attempt});
                        } catch(e) {
                            res[i] = {error: e};
                            if (/e(socket)?timedout/i.test(e.code) &&
                                (opt.retries<0 || attempt<=opt.retries))
                            {
                                continue;
                            }
                            handler.emit('failure', e, {url: url, proxy: proxy,
                                index: i, attempt: attempt});
                        }
                        delete requests[i];
                        break;
                    }
                }
            }));
        }
        yield etask.all(queue);
        if (!urls)
            return handler.emit('abort');
        handler.emit('end', res);
    });
    return handler;
};
