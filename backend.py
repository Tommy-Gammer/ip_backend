import os
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS
import mysql.connector
from mysql.connector import pooling

load_dotenv()

app = Flask(__name__)
CORS(app)

dbconfig = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "database": os.getenv("DB_NAME", "sakila"),
    "charset": "utf8mb4",
    "use_pure": True,
}
cnxpool = pooling.MySQLConnectionPool(pool_name="sakila_pool", pool_size=10, **dbconfig)

def db():
    conn = cnxpool.get_connection()
    cur = conn.cursor(dictionary=True)
    return conn, cur

# -------------------------------
# Home Page Endpoints
# -------------------------------

@app.get("/api/films/top-rented")
def top_rented_films():
    sql = """
      SELECT film.film_id, film.title, film.description, film.release_year, film.length, film.rating, category.name AS category, COUNT(rental.rental_id) AS rental_count
      FROM film
      JOIN inventory ON inventory.film_id = film.film_id
      JOIN rental ON rental.inventory_id = inventory.inventory_id
      JOIN film_category ON film_category.film_id = film.film_id
      JOIN category ON category.category_id = film_category.category_id
      GROUP BY film.film_id, film.title, film.description, film.release_year, film.length, film.rating, category.name
      ORDER BY rental_count DESC LIMIT 5;
    """
    conn, cur = db()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(result)

@app.get("/api/actors/top")
def top_actors_in_store():
    sql = """
      SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(DISTINCT inventory.film_id) AS film_count
      FROM actor
      JOIN film_actor ON film_actor.actor_id = actor.actor_id
      JOIN inventory ON inventory.film_id = film_actor.film_id
      GROUP BY actor.actor_id, actor.first_name, actor.last_name
      ORDER BY film_count DESC, actor.last_name, actor.first_name LIMIT 5;
    """
    conn, cur = db()
    cur.execute(sql)
    result = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(result)

@app.get("/api/films/<int:film_id>")
def film_details(film_id: int):
    sql = """
      SELECT film.film_id, film.title, film.description, film.release_year, film.length, film.rating, category.name AS category, COUNT(rental.rental_id) AS rental_count
      FROM film
      LEFT JOIN inventory ON inventory.film_id = film.film_id
      LEFT JOIN rental ON rental.inventory_id = inventory.inventory_id
      LEFT JOIN film_category ON film_category.film_id = film.film_id
      LEFT JOIN category ON category.category_id = film_category.category_id
      WHERE film.film_id = %s
      GROUP BY film.film_id, film.title, film.description, film.release_year, film.length, film.rating, category.name;
    """
    conn, cur = db()
    cur.execute(sql, (film_id,))
    film = cur.fetchone()
    cur.close()
    conn.close()
    return jsonify(film)

@app.get("/api/actors/<int:actor_id>")
def actor_details(actor_id: int):
    sql_actor = """
      SELECT actor.actor_id, actor.first_name, actor.last_name, COUNT(DISTINCT film_actor.film_id) AS film_count
      FROM actor
      LEFT JOIN film_actor ON film_actor.actor_id = actor.actor_id
      WHERE actor.actor_id = %s
      GROUP BY actor.actor_id, actor.first_name, actor.last_name;
    """
    sql_top_films = """
      SELECT film.film_id, film.title, COUNT(rental.rental_id) AS rental_count
      FROM film
      JOIN film_actor ON film_actor.film_id = film.film_id
      JOIN inventory ON inventory.film_id = film.film_id
      JOIN rental ON rental.inventory_id = inventory.inventory_id
      WHERE film_actor.actor_id = %s
      GROUP BY film.film_id, film.title
      ORDER BY rental_count DESC LIMIT 5;
    """
    conn, cur = db()
    cur.execute(sql_actor, (actor_id,))
    actor = cur.fetchone()
    cur.execute(sql_top_films, (actor_id,))
    actor["top_movies"] = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify(actor)

# -------------------------------
# Film Page Endpoints
# -------------------------------

# -------------------------------
# Customer Page Endpoints
# -------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
