#!/bin/env python3

import json
import math
import mmh3
import os
import requests
import socket
from flask import Flask, request, abort

# -------------------------------------------------------------------------


def own_name():
    """Return to the name of the pod in which the server instace is running."""
    # TODO: Implement it using self-awarness patterns.
    return name

def get_namespace():
    """Return to the namespace in which the server instace is running."""
    # TODO: Implement it using self-awarness patterns.
    return name

def pod_names():
    """Return pod names of the deployment."""
    # TODO
    #
    # The implementation (here and elsewhere) should not make any
    # assumptions on the number of pods in the system.
    return names

def remote_addr(pod_name):
    """
    Return the fully qualified domain name or the IP address of POD_NAME.
    POD_NAME is a member of the list returned by pod_names().
    """
    # TODO
    return addr

def load_db():
    """Load the database (a dict object) from hard disk."""
    # TODO: Modify the code if necessary
    try:
        with open('/db/a.json') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def save_db(db):
    """Save the database (a dict object) to hard disk."""
    # TODO: Modify the code if necessary
    with open('/db/a.json', 'w') as f:
        return json.dump(db, f)

# -------------------------------------------------------------------------
# Do not modify anything below this line
#
# It is tempting to change the arguments of app.run() below.  Do not
# that.  Instead write a pod specification that passes the correct
# command line arguments to this program.

# These functions are based on the code of
# https://en.wikipedia.org/wiki/Rendezvous_hashing

def hash_to_unit_interval(s):
    """Hashes a string onto the unit interval (0, 1]"""
    return (mmh3.hash128(s) + 1) / 2**128

def compute_score(node_name, key):
    score = hash_to_unit_interval(f"{node_name}: {key}")
    log_score = 1.0 / -math.log(score)
    return log_score

def determine_responsible_node(nodes, key):
    """Determines which node of a set of NODES is responsible for KEY."""
    return max(
        nodes, key=lambda node: compute_score(node, key), default=None)

# -------------------------------------------------------------------------

def get_location(key):
    return determine_responsible_node(pod_names(), key)

def get_object_value(key):
    loc = get_location(key)
    if loc == own_name():
        db = load_db()
        print(db)
        val = db.get(key)
        if val is None:
            abort(404)
        return val
    else:
        addr  = remote_addr(loc)
        url = f"http://{addr}:5000/obj/{key}"
        ret = requests.get(url)
        if ret.status_code != 200:
            abort(ret.status_code)
        return ret.text

def set_object_value(key, value):
    loc = get_location(key)
    if loc == own_name():
        db = load_db()
        db[key] = value
        save_db(db)
        return ''
    else:
        addr = remote_addr(loc)
        url = f"http://{addr}:5000/obj/{key}/{value}"
        ret = requests.get(url)
        if ret.status_code != 200:
            abort(ret.status_code)
        return ret.text

# -------------------------------------------------------------------------

app = Flask(__name__)

@app.route("/obj/<key>")
def get_object(key=None):
    return get_object_value(key)

@app.route("/obj/<key>/<val>")
def set_object(key=None, val=None):
    return set_object_value(key, val)

@app.get("/location/<key>")
def location(key=None):
    return get_location(key)

@app.route("/name")
def name():
    return own_name()

@app.route("/pod-names")
def names():
    return pod_names()

@app.route("/hello")
def hello_world():
    return "<p>Hello, World!</p>"

if __name__ == "__main__":
    app.run(debug=True, port=5000)
