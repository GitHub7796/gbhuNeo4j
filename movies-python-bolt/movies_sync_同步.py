#!/usr/bin/env python
import logging
import os
from json import dumps
# 格式化文本
from textwrap import dedent
# 用于支持类型提示（Type Hints）。
# 类型提示是在函数声明、变量声明等地方添加类型信息，以帮助开发者更好地理解和检查代码
from typing import cast
# Python 的 typing 模块的扩展，提供了额外的类型提示工具，用于更丰富、更复杂的类型标注。
# 这个模块通常用于支持一些高级的类型提示场景，特别是在处理泛型和类型变量时
from typing_extensions import LiteralString
# 轻量级的 Python Web 框架
from flask import Flask, Response, request
import neo4j
from neo4j import GraphDatabase, basic_auth


app = Flask(__name__, static_url_path="/static/")

url = os.getenv("NEO4J_URI", "neo4j+s://demo.neo4jlabs.com")
username = os.getenv("NEO4J_USER", "movies")
password = os.getenv("NEO4J_PASSWORD", "movies")
neo4j_version = os.getenv("NEO4J_VERSION", "4")
database = os.getenv("NEO4J_DATABASE", "movies")

port = int(os.getenv("PORT", 8080))

driver = GraphDatabase.driver(url, auth=basic_auth(username, password))


def query(q: LiteralString) -> LiteralString:
    # this is a safe transform:
    # no way for cypher injection by trimming whitespace
    # hence, we can safely cast to LiteralString
    return cast(LiteralString, dedent(q).strip())


@app.route("/")
def get_index():
    return app.send_static_file("index.html")


def serialize_movie(movie):
    return {
        "id": movie["id"],
        "title": movie["title"],
        "summary": movie["summary"],
        "released": movie["released"],
        "duration": movie["duration"],
        "rated": movie["rated"],
        "tagline": movie["tagline"],
        "votes": movie.get("votes", 0)
    }


def serialize_cast(cast):
    return {
        "name": cast[0],
        "job": cast[1],
        "role": cast[2]
    }


@app.route("/graph")
def get_graph():
    records, _, _ = driver.execute_query(
        query("""
            MATCH (m:Movie)<-[:ACTED_IN]-(a:Person)
            RETURN m.title AS movie, collect(a.name) AS cast
            LIMIT $limit
        """),
        database_=database,
        routing_="r",
        limit=request.args.get("limit", 100)
    )
    nodes = []
    rels = []
    i = 0
    for record in records:
        nodes.append({"title": record["movie"], "label": "movie"})
        target = i
        i += 1
        for name in record["cast"]:
            actor = {"title": name, "label": "actor"}
            try:
                source = nodes.index(actor)
            except ValueError:
                nodes.append(actor)
                source = i
                i += 1
            rels.append({"source": source, "target": target})
    return Response(dumps({"nodes": nodes, "links": rels}),
                    mimetype="application/json")


@app.route("/search")
def get_search():
    try:
        q = request.args["q"]
    except KeyError:
        return []
    else:
        records, _, _ = driver.execute_query(
            query("""
                MATCH (movie:Movie)
                WHERE toLower(movie.title) CONTAINS toLower($title)
                RETURN movie
            """),
            title=q,
            database_=database,
            routing_="r",
        )
        return Response(
            dumps([serialize_movie(record["movie"]) for record in records]),
            mimetype="application/json"
        )


@app.route("/movie/<title>")
def get_movie(title):
    result = driver.execute_query(
        query("""
            MATCH (movie:Movie {title:$title})
            OPTIONAL MATCH (movie)<-[r]-(person:Person)
            RETURN movie.title as title,
            COLLECT(
                [person.name, HEAD(SPLIT(TOLOWER(TYPE(r)), '_')), r.roles]
            ) AS cast
            LIMIT 1
        """),
        title=title,
        database_=database,
        routing_="r",
        result_transformer_=neo4j.Result.single,
    )
    if not result:
        return Response(dumps({"error": "Movie not found"}), status=404,
                        mimetype="application/json")

    return Response(dumps({"title": result["title"],
                           "cast": [serialize_cast(member)
                                    for member in result["cast"]]}),
                    mimetype="application/json")


@app.route("/movie/<title>/vote", methods=["POST"])
def vote_in_movie(title):
    summary = driver.execute_query(
        query("""
            MATCH (m:Movie {title: $title})
            SET m.votes = coalesce(m.votes, 0) + 1;
        """),
        database_=database,
        title=title,
        result_transformer_=neo4j.Result.consume,
    )
    updates = summary.counters.properties_set
    return Response(dumps({"updates": updates}), mimetype="application/json")


if __name__ == "__main__":
    logging.root.setLevel(logging.INFO)
    logging.info("Starting on port %d, database is at %s", port, url)
    try:
        app.run(port=port)
    finally:
        driver.close()
