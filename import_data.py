#!/usr/bin/python3
import sys
import re
import pymysql


def main():
    try:
        connection = pymysql.connect(
                host='localhost',
                user='root',
                password = "SECRETPASSWORD",
                db='RU_mobilization',
                cursorclass=pymysql.cursors.DictCursor
            )

        DB_cursor  = connection.cursor()
    except BaseException:
        sys.exit("Cannot connect to DB")

    try:
        f = open("Priziv1volna.txt","r")
    except BaseException:
        sys.exit("Cannot open file")

    lines = f.readlines()
    n_entries = len(lines)
    current_line = 1
    errors = 0
    soldier_entry = r'^(?P<surname>\S*|(\S*\s{1}\S*)|(\S*\s{1}\S*\s{1}\S*))\s{1}(?P<name>\S*)\s{1}(?P<patronymic>\S*)\s*(?P<DOB>\d{1,2}\.\d{1,2}\.\d{4})\s*(?P<address>.*|\s{1})\s(?P<id>\d{10})\s*(?P<office>.*)$'
    soldier_entry_re = re.compile(soldier_entry)

    for line in lines:
        print(
            "Processing line " +
            str(current_line) +
            " of " +
            str(n_entries)
        )
        try:
            soldier_matched = soldier_entry_re.match(line)
            surname = soldier_matched.group('surname')
            name = soldier_matched.group('name')
            patronymic = soldier_matched.group('patronymic')
            DOB_raw = soldier_matched.group('DOB')
            DOB_raw_list = DOB_raw.split(".")
            DOB = DOB_raw_list[2] + "-" + DOB_raw_list[1] + "-" + DOB_raw_list[0]
            address = soldier_matched.group('address')
            soldier_id = soldier_matched.group('id')
            office = soldier_matched.group('office')
            sql = "INSERT INTO mobilized(name, surname, patronymic, DOB, address, id, office) VALUES ('" + \
                name + \
                "', '" + \
                surname + \
                "', '" + \
                patronymic + \
                "', '" + \
                DOB + \
                "', '" + \
                address + \
                "', '" + \
                soldier_id + \
                "', '" + \
                office + \
                "');"
            DB_cursor.execute(sql)
            connection.commit()
            print("Line " +
                str(current_line) +
                " commited to DB"
            )
        except BaseException:
            print(
                "Cannot process line " +
                str(current_line)
            )
            print(sql)
            errors += 1

        current_line += 1

    # We have iterated all lines
    connection.close()

    if errors == 0:
        print(
            "All " +
            str(n_entries) +
            " processed correctly"
        )
    else:
        print(
            str(errors) +
            " lines out of " +
            str(n_entries) +
            " not processed correctly"
        )


if __name__ == '__main__':
    main()