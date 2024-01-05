do $$
declare
    genre_name char(100);
begin
    genre_name := 'Genre';
    for i in 100..120
        loop
            insert into genre(genre_id, genre_name)
            values(i, genre_name || ' ' || i);
        end loop;
end;
$$