package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/jackc/pgx/v4/stdlib"
)

func main() {

	//Connect to a database

	conn, err := sql.Open("pgx", "host=localhost port=5432 dbname=users user=xyz password")
	if err != nil {

		log.Fatal(fmt.Sprintf("Unable to connect: %v\n", err))

	}

	defer conn.Close()

	log.Println("Connected to database")

	//test my connection

	err = conn.Ping()
	if err != nil {
		log.Fatal("Cannot ping Database")
	}

	log.Println("Ping Database")

	//get rows from table

	err = getAllrows(conn)
	if err != nil {
		log.Println(err)
	}

	//Insert a row

	query := `insert into names (first_name,last_name) values ($1,$2)`
	_, err = conn.Exec(query, "Milli", "Brown")
	if err != nil {

		log.Fatal(err)
	}

	log.Println("Inserted a row")

	//get rows from table again
	err = getAllrows(conn)
	if err != nil {
		log.Println(err)
	}

	//update a rows

	stmt := `update names set first_name=$1 where first_name= $2`
	_, err = conn.Exec(stmt, "Darla", "Milli")
	if err != nil {

		log.Fatal(err)
	}

	//get rows from table again
	err = getAllrows(conn)
	if err != nil {
		log.Println(err)
	}

	//update a row

	stmt = `update names set first_name=$1 where id= $2`
	_, err = conn.Exec(stmt, "DIA", 3)
	if err != nil {

		log.Fatal(err)
	}

	//get rows from table again
	err = getAllrows(conn)
	if err != nil {
		log.Println(err)
	}

	log.Println("Updated one or more rows")

	//get one row by id
	var firstName, lastName string
	var id int
	query = `select id,first_name,last_name from names where first_name = $1`
	row := conn.QueryRow(query,"DIA")
	err = row.Scan(&id, &firstName, &lastName)
	if err != nil {
		log.Fatal(err)
	}

	
	fmt.Println("Record is **************** ", id, firstName, lastName)

	//delete a row
	query = `delete from names where id=$1`
	_, err = conn.Exec(query,5)
	if err != nil {
		log.Fatal(err)
	}

	//get rows again
	err = getAllrows(conn)
	if err != nil {
		log.Println(err)
	}

}

func getAllrows(conn *sql.DB) error {

	rows, err := conn.Query("select id,first_name,last_name from names")
	if err != nil {

		log.Println(err)
		return err
	}

	defer rows.Close()

	var firstName, lastName string
	var id int

	for rows.Next() {

		err := rows.Scan(&id, &firstName, &lastName)
		if err != nil {
			log.Println(err)
			return err
		}

		fmt.Println("Record is ", id, firstName, lastName)
	}

	if err = rows.Err(); err != nil {

		log.Fatal("Error scanning rows", err)
	}

	fmt.Println("----------------------------------------------------------------------------")

	return nil
}
