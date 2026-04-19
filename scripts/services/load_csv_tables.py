import psycopg2 as psycopg
import os, io

def load_images_bbox(file_path:str, pg_params):
    with psycopg.connect(**pg_params) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                
                buff = io.BytesIO(content.encode("utf-8"))
                pg_cur.execute("""
                    CREATE TEMP TABLE t_bbox(
                        image_name VARCHAR(64),
                        x_coord INT,
                        y_coord INT,
                        width INT,
                        height INT         
                    ) ON COMMIT DROP;
                """)

                pg_cur.copy_expert("""
                    COPY t_bbox (image_name, x_coord, y_coord, width, height)
                    FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')   
                """, buff)

                pg_cur.execute("""
                    WITH cte AS (
                        SELECT 
                            i.image_id,
                            tmp.x_coord,
                            tmp.y_coord,
                            tmp.width,
                            tmp.height
                        FROM t_bbox tmp
                        JOIN uploaded_images.images i
                            ON i.image_name = tmp.image_name           
                    )
                    INSERT INTO uploaded_images.image_face_bbox
                    SELECT 
                        cte.image_id,
                        cte.x_coord,
                        cte.y_coord,
                        cte.width,
                        cte.height
                    FROM cte
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM uploaded_images.image_face_bbox ifb
                        WHERE cte.image_id = ifb.image_id
                    )
                """)
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                raise Exception(f"Error while integrating bbox coordinates: {e}")


def load_images_eyes_coord(file_path:str, pg_params):
    with psycopg.connect(**pg_params) as pg_conn:
        with pg_conn.cursor() as pg_cur:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    content = f.read()
                
                buff = io.BytesIO(content.encode("utf-8"))
                pg_cur.execute("""
                    CREATE TEMP TABLE t_eyes_coords(
                        image_name VARCHAR(64),
                        lefteye_x INT,
                        lefteye_y INT,
                        righteye_x INT,
                        righteye_y INT,
                        nose_x INT,
                        nose_y INT,
                        leftmouth_x INT,
                        leftmouth_y INT,
                        rightmouth_x INT,
                        rightmouth_y INT       
                    ) ON COMMIT DROP;
                """)

                pg_cur.copy_expert("""
                    COPY t_eyes_coords (image_name,lefteye_x,lefteye_y,righteye_x,righteye_y,nose_x,nose_y,leftmouth_x,leftmouth_y,rightmouth_x,rightmouth_y)
                    FROM STDIN WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')   
                """, buff)

                pg_cur.execute("""
                    WITH cte AS (
                        SELECT 
                            i.image_id,
                            tmp.lefteye_x,
                            tmp.lefteye_y,
                            tmp.righteye_x,
                            tmp.righteye_y
                        FROM t_eyes_coords tmp
                        JOIN uploaded_images.images i
                            ON i.image_name = tmp.image_name           
                    )
                    INSERT INTO uploaded_images.image_eyes_coords
                    SELECT 
                        cte.image_id,
                        cte.lefteye_x,
                        cte.lefteye_y,
                        cte.righteye_x,
                        cte.righteye_y
                    FROM cte
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM uploaded_images.image_eyes_coords iec
                        WHERE cte.image_id = iec.image_id
                    )
                """)
                pg_conn.commit()
            except Exception as e:
                pg_conn.rollback()
                raise Exception(f"Error while integrating eyes coordinates: {e}")


if __name__ == '__main__':
    dir_path = "data/celeba"     
    filename = "list_landmarks_align_celeba.csv"      
    file_path = os.path.join(dir_path, filename)
    conn = {
        'host': "localhost",     
        'port': 5432,             
        'dbname': "images",   
        'user': "seeker",    
        'password': "secret_pass", 
        'connect_timeout': 10     
    }
    load_images_eyes_coord(file_path, conn)