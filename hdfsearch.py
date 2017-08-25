
import json
from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request
from flasgger import Swagger
from hdfs3 import HDFileSystem

app = Flask(__name__)
swagger = Swagger(app)


def get_hdfs():
    hdfs = HDFileSystem(host='localhost', port=9000)
    return hdfs

def get_es():
    es = Elasticsearch()
    return es


@app.route('/resource', methods=['GET'])
def list_resources():
    """Endpoint return a list of resources in HDFS.
    ---
    definitions:
        Resource:
            type: string
            properties:
                resourcename:
                    type: string
    """

    hdfs = get_hdfs()
    paths = hdfs.ls('/user/hdfsearch/')
    print(paths)

    return jsonify(paths)


@app.route('/resource', methods=['POST'])
def create_resource():
    """Endpoint to describe table
    ---
    parameters:
      - name: params
        in: body
        required: true
        description: Resource folder in HDFS.
        schema:
          required:
            - resourcename
            - splitby
          properties:
            resourcename:
              default: resourcefolder
            splitby:
              default: splitfield
    """

    params = request.get_json()
    path = params['resourcename']

    hdfs = get_hdfs()

    hdfs.mkdir(path)
    with hdfs.open(path + '/.splitby', 'wb') as f:
        f.write(params['splitby'])

    return jsonify({ 'result': 'ok' })


@app.route('/resource/<resource>', methods=['DELETE'])
def remove_resource(resource):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
    """
    hdfs = get_hdfs()

    hdfs.rm(resource, recursive=True)

    return jsonify({ 'result': 'ok' })


@app.route('/resource/<resource>', methods=['GET'])
def get_resource_files(resource):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
    """

    hdfs = get_hdfs()

    files = hdfs.ls(resource)

    return jsonify(files)


@app.route('/resource/<resource>', methods=['POST'])
def send_data(resource):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
      - name: params
        in: body
        required: true
        description: Data to save in HDFS folder.
        schema:
          required:
            - data
          properties:
            data:
              default: [{ field: value}]
    """

    # TODO: Handle errors
    # TODO: Report when data have no splitby key

    data = request.get_json()['data']

    hdfs = get_hdfs()
    splitby = hdfs.open(resource + '/.splitby').read()

    splitted_data = {}
    for e in data:
        key_value = e[splitby]
        if not key_value in splitted_data:
            splitted_data[key_value] = []
        splitted_data[key_value].append(e)

    for key in splitted_data:
        file_name = resource + '-' + key
        with hdfs.open(resource + '/' + file_name, 'wb') as f:
            for e in splitted_data[key]:
                f.write(json.dumps(e) + '\n')

    print('send_data()')
    print('resource %s splitby %s' % (resource, splitby))
    #print('data', data)

    return jsonify({ 'result' : 'ok' })


@app.route('/resource/<resource>/<filename>', methods=['GET'])
def get_resource_file_data(resource, filename):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
      - name: filename
        in: path
        type: string
        required: true
        description: Resource filename in folder in HDFS.
    """

    hdfs = get_hdfs()

    with hdfs.open(resource + '/' + filename) as f:
        file_data = f.read()

    lines = file_data.splitlines()
    data = []

    for l in lines:
        data.append(json.loads(l))

    return jsonify(data)


@app.route('/resource/<resource>/<filename>/index', methods=['GET'])
def get_indexed_resource_file(resource, filename):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
      - name: filename
        in: path
        type: string
        required: true
        description: Resource filename in folder in HDFS.
    """

    hdfs = get_hdfs()
    es = get_es()

    index_name = resource + '-' + filename
    indexed_file_flag = resource + '/' + filename + '.indexed'

    if not hdfs.exists(indexed_file_flag):
        es.indices.create(index=index_name, ignore=400)
        hdfs.touch(indexed_file_flag)
        with hdfs.open(resource + '/' + filename) as f:
            file_data = f.read()

        lines = file_data.splitlines()

        for l in lines:
            doc = json.loads(l)
            es.index(index=index_name, doc_type=filename, body=doc)

    result = es.search(index=index_name, doc_type=filename)

    return jsonify(result)


@app.route('/resource/<resource>/<filename>/index', methods=['DELETE'])
def delete_indexed_resource_file(resource, filename):
    """Endpoint to describe table
    ---
    parameters:
      - name: resource
        in: path
        type: string
        required: true
        description: Resource folder in HDFS.
      - name: filename
        in: path
        type: string
        required: true
        description: Resource filename in folder in HDFS.
    """

    hdfs = get_hdfs()
    es = get_es()

    index_name = resource + '-' + filename
    indexed_file_flag = resource + '/' + filename + '.indexed'

    es.indices.delete(index=index_name)
    hdfs.rm(indexed_file_flag)

    return jsonify({ "result": "ok" })

def main():
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=True
    )


if __name__ == '__main__':
    main()

