import pandas as pd 
from rest_framework.decorators import api_view
from rest_framework.response import Response
import psycopg2


@api_view(['POST'])
def upload_file (request):
    try:
        # Get the database credentials from the form-data
        db_host = request.POST.get('DB_HOST')
        db_name = request.POST.get('DB_NAME')
        db_user = request.POST.get('DB_USER')
        db_password = request.POST.get('DB_PASSWORD')
        db_table_name = request.POST.get('DB_TABLE_NAME')
        csv_file=request.FILES.get('CSV_FILE')
        type_OF_FILE=str(csv_file)
        # Check the type of file 
        if type_OF_FILE.endswith('.csv'):   
            df = pd.read_csv(csv_file)
            connection = psycopg2.connect(
                host=db_host,
                dbname=db_name,
                user=db_user,
                password=db_password
            )
            if connection:
            # Get the column names of the database table & store them into the list 
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{db_table_name}'")
                    table_columns = [column[0] for column in cursor.fetchall()]                   
                    if table_columns:
                        cursor.execute(f"DELETE FROM {db_table_name}")
                        Common_colom = ''
                        # Itrate the columns of the dataframe 
                        for col_name in df.columns:
                            # Check the dataframe colomn present in the table and store
                            if col_name in table_columns:
                                if Common_colom:
                                    Common_colom = Common_colom + ',' + col_name
                                else:
                                    Common_colom = col_name
                            # Remove the extra columns from the dataframe 
                            else :
                                df.drop([col_name], axis=1, inplace=True)
                        # Convert the row into the tuple and store it into the database
                        for row in df.itertuples(index=False):
                            row=tuple(row)
                            insert_query = f"INSERT INTO {db_table_name} ({Common_colom}) VALUES %s"
                            cursor.execute(insert_query,[row])
                        connection.commit()
                # Close the connection
                        connection.close()
                        return Response({"message": "DataFrame data inserted into the database"})
                    connection.close()
                    return Response({"message": "Table not present into the database"})
            return Response({"message":"Credentials can't match please check it and try again"})                
        return Response({"message": "Please provide the csv file "})
    except Exception as e:
        return Response({"error":str(e)}, status=200)


                    





