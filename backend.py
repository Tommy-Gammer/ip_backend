import os
from dotenv import load_dotenv
from flask import Flask, jsonify, request
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
@app.get("/api/films/search")
def search_films():
    by = (request.args.get("by") or "all").lower()
    q  = (request.args.get("q") or "").strip()
    like = f"%{q}%"

    sql_base = """
      SELECT film.film_id, film.title, film.description, film.release_year, film.length, film.rating,
            categories_single.category_name AS category, COALESCE(rentals_per_film.rental_count, 0) AS rental_count,
            actors_per_film.actors AS actors
      FROM film
      LEFT JOIN (SELECT inventory.film_id AS film_id, COUNT(rental.rental_id) AS rental_count
                FROM inventory JOIN rental ON rental.inventory_id = inventory.inventory_id
                GROUP BY inventory.film_id) AS rentals_per_film ON rentals_per_film.film_id = film.film_id
      LEFT JOIN (SELECT film_category.film_id AS film_id, MIN(category.name) AS category_name
                FROM film_category JOIN category ON category.category_id = film_category.category_id
                GROUP BY film_category.film_id) AS categories_single ON categories_single.film_id = film.film_id
      LEFT JOIN (SELECT film_actor.film_id AS film_id,
                GROUP_CONCAT(DISTINCT CONCAT(actor.first_name, ' ', actor.last_name)
                ORDER BY actor.last_name, actor.first_name SEPARATOR ', ') AS actors
                FROM film_actor
                JOIN actor ON actor.actor_id = film_actor.actor_id
                GROUP BY film_actor.film_id) AS actors_per_film ON actors_per_film.film_id = film.film_id
    """

    where_clause = ""
    params = []

    if by == "film" and q:
        where_clause = "WHERE film.title LIKE %s"
        params = [like]
    elif by == "actor" and q:
        where_clause = """
          WHERE EXISTS (
            SELECT 1
            FROM film_actor
            JOIN actor ON actor.actor_id = film_actor.actor_id
            WHERE film_actor.film_id = film.film_id
              AND (actor.first_name LIKE %s OR actor.last_name LIKE %s OR CONCAT(actor.first_name, ' ', actor.last_name) LIKE %s)
          )
        """
        params = [like, like, like]
    elif by == "genre" and q:
        where_clause = """
          WHERE EXISTS (
            SELECT 1
            FROM film_category
            JOIN category ON category.category_id = film_category.category_id
            WHERE film_category.film_id = film.film_id
              AND category.name LIKE %s
          )
        """
        params = [like]

    sql = f"""{sql_base} {where_clause} ORDER BY film.title ASC;"""
    conn, cur = db()
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return jsonify(rows)

@app.post("/api/films/rent")
def rent_film():
    data = request.get_json() or {}
    film_id = int(data.get("film_id") or 0)
    customer_id = int(data.get("customer_id") or 0)

    if not film_id or not customer_id:
        return jsonify({"ok": False, "error": "film_id and customer_id are required"}), 400

    conn, cur = db()
    try:
        # check if customer exists
        cur.execute("SELECT 1 FROM customer WHERE customer_id = %s", (customer_id,))
        if not cur.fetchone():
            return jsonify({"ok": False, "error": "Customer ID does not exist"}), 404

        # check if copy is available
        cur.execute("""
          SELECT inventory.inventory_id
          FROM inventory
          LEFT JOIN rental ON rental.inventory_id = inventory.inventory_id AND rental.return_date IS NULL
          WHERE inventory.film_id = %s AND rental.inventory_id IS NULL
          LIMIT 1;
        """, (film_id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"ok": False, "error": "No copies available in store"}), 409

        inventory_id = row["inventory_id"]

        # get next rental_id
        cur.execute("SELECT COALESCE(MAX(rental_id), 0) + 1 AS next_id FROM rental;")
        next_row = cur.fetchone()
        next_id = next_row["next_id"]

        # create rental
        cur.execute("""
          INSERT INTO rental (rental_id, rental_date, inventory_id, customer_id, staff_id)
          VALUES (%s, UTC_TIMESTAMP(), %s, %s, 1);
        """, (next_id, inventory_id, customer_id))
        conn.commit()

        return jsonify({
            "ok": True,
            "rental_id": next_id,
            "film_id": film_id,
            "customer_id": customer_id,
            "inventory_id": inventory_id
        })
    finally:
        cur.close()
        conn.close()


# -------------------------------
# Customer Page Endpoints
# -------------------------------

if __name__ == "__main__":
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
