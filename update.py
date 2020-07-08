#!/usr/bin/env python3
import argparse
import configparser
import csv
import sqlite3
import datetime

def create_user_database(db_file):
	db = sqlite3.connect(db_file, 
		detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
	cursor = db.cursor()
	cursor.execute('''CREATE TABLE IF NOT EXISTS zoom_users (email text,
		firstname text,
		lastname text,
		department text,
		usergroup text,
		imgroup text,
		lastlogin timestamp,
		lastclient text,
		creationdate timestamp,
		role text,
		usertype text,
		logintype text,
		userstatus text,
		pmi text,
		jobtitle text,
		location text)''')
	db.commit()
	return db

def parse_user_export(db, export):
	cursor = db.cursor()
	with open(export, encoding='utf-8-sig') as csvfile:
		reader = csv.DictReader(csvfile, delimiter=',')
		for row in reader:
			if row['Last Login(UTC)'] != '':
				lastlogin = datetime.datetime.strptime(row['Last Login(UTC)'],'%Y-%m-%d %H:%M:%S')
			else:
				lastlogin = ''
			values = (row['Email'],
			row['First Name'],
			row['Last Name'],
			row['Department'],
			row['User Group'],
			row['IM Group'],
			lastlogin,
			row['Last Client Version'],
			datetime.datetime.strptime(row['Creation Date'],'%Y-%m-%d'),
			row['Role'],
			row['User Type'],
			row['Login Type'],
			row['User Status'],
			row['PMI'],
			row['Job Title'],
			row['Location'])

			cursor.execute('''SELECT email FROM zoom_users WHERE email = ?''',(values[0],))
			cursor.execute('''INSERT INTO zoom_users 
				VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
				values)

		db.commit()

def create_user_update_file(db, output, year):
	date = (year + '-01-01',)
	print(date)
	with open(output,'w') as csvfile:
		writer = csv.DictWriter(csvfile,quoting=csv.QUOTE_NONE,fieldnames=['Email',
			'First Name',
			'Last Name',
			'Phone Number',
			'Department',
			'User Type',
			'Large Meeting',
			'Webinar',
			'Job Title',
			'Location'])
		writer.writeheader()
		cursor = db.cursor()
		cursor.execute('''SELECT email,firstname,lastname,department,jobtitle,location,lastlogin 
			FROM zoom_users
			WHERE lastlogin < date(?)''', date)
		data = cursor.fetchall()
		print(len(data))
		for row in data:
			#print(row)
			writer.writerow({'Email':row[0],
				'First Name':row[1],
				'Last Name':row[2],
				'Phone Number':'',
				'Department':row[3],
				'User Type':'Basic',
				'Large Meeting':'',
				'Webinar':'',
				'Job Title':row[4],
				'Location':row[5]})

if __name__ == "__main__":
	# argparse stuff
	parser = argparse.ArgumentParser(description='format a csv file for updating users in Zoom')
	parser.add_argument('config')
	parser.add_argument('zoom_export')
	args = parser.parse_args()

	print(args.config)

	config = configparser.ConfigParser()
	config.read(args.config)

	db = create_user_database(config['Paths'].get('database'))
	parse_user_export(db, args.zoom_export)
	create_user_update_file(db, 'out.csv', config['Data'].get('year'))
