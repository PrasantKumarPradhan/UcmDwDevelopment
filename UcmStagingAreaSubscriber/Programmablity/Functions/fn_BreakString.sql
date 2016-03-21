--This function accepts a string, splits it into substrings based on a delimiter, and returns the substrings in a table.
create function dbo.fn_BreakString (@string nvarchar(4000), @delimiter nvarchar(255) = N',') 
   returns @t table (id int identity primary key, string nvarchar(4000)) as
begin
   declare @start int
   declare @length int
   select @string = replace(@string, @delimiter, nchar(5))
   select @delimiter = nchar(5)
   select @start = 1
   select @length = charindex(@delimiter, @string, @start) + 1 - @start
   if @length <= 0 and datalength(@string) >= @start
      select @length = datalength(@string) + 3 - @start
   while @length > 0
   begin
      insert into @t (string) values (ltrim(rtrim(substring(@string, @start, @length - 1))))
      select @start = @start + @length
      select @length = charindex(@delimiter, @string, @start) + 1 - @start
      if @length <= 0 and datalength(@string) >= @start
         select @length = datalength(@string) + 3 - @start
   end
   return
end
