# Creating database and tables to test the code #
-------------------------------------------------------------------------

create database Cellphones
use Cellphones

Create table Dim_Date
(
[Date] date primary key,
[Year] As Datename(yyyy,[Date]),
[Quarter] As 'Quarter ' + Convert(nvarchar(20),Datename(qq,[Date])),
[Month] As Convert(nvarchar(20),Datename(mm,[Date]))
)

create table Dim_Customer
(
IdCustomer int primary key,
Customer_name nvarchar(30) not null,
Email nvarchar(100) not null,
Phone int not null
)

Create table Dim_Location
(
IdLocation int primary key,
[Zip Code] int not null,
Country nvarchar(20),
[State] nvarchar(30) not null,
City nvarchar(20) not null
)

Create table Dim_Manufacturer
(
IdManufacturer int primary key,
Manufacturer_Name nvarchar(20)
)

Create table Dim_Model
(
IdModel int primary key,
Model_Name nvarchar(20) not null,
Unit_Price money not null,
IdManufacturer int not null
)

Create table Fact_Transaction
(
IdModel int not null,
IdCustomer int not null,
IdLocation int not null,
[Date] Date not null,
[Total Price] money not null,
Quantity int not null
)

alter table Fact_Transaction add constraint FK_customerid
Foreign key (IdCustomer) references Dim_Customer (IdCustomer)

alter table Fact_Transaction add constraint FK_locationid
Foreign key (IdLocation) references Dim_Location (IdLocation)

alter table Fact_Transaction add constraint FK_modelid
Foreign key (IdModel) references Dim_Model (IdModel)

alter table Fact_Transaction add constraint FK_date
Foreign key ([Date]) references Dim_Date ([Date])

alter table Dim_Model add constraint FK_manufacturerid
Foreign key (IdManufacturer) references Dim_Manufacturer (IdManufacturer)

alter table Dim_Customer
add constraint U_Email
unique (Email)

alter table Dim_Location
add constraint U_Zipcode
unique ([Zip Code])

insert into Dim_Location values
(1,10001,'US','New York','New York City'),
(2,10002,'US','New Jersey','Trenton'),
(3,10003,'US','Washington','Olympia'),
(4,10004,'US','Oregon','Salem'),
(5,10005,'US','California','Sacramento'),
(6,10006,'US','Idaho','Boise'),
(7,10007,'US','Nevada','Carson City'),
(8,10008,'US','Arizona','Pheonix'),
(9,10009,'US','Utah','Salt Lake City'),
(10,10010,'US','Texas','Austin'),
(11,10011,'US','Colorado','Denver'),
(12,10012,'US','Minnesota','St Paul'),
(13,10013,'US','Illinois','Chicago'),
(14,10014,'US','Massachusetts','Boston'),
(15,10015,'US','Florida','Miami')

insert into Dim_Customer values
(1,'Aron','aron@gmail.com',35245),
(2,'Blake','blake@ymail.com',32489),
(3,'Chelsey','chelsey@aol.com',34373),
(4,'Dave','dave@gmail.com',29382),
(5,'Ellie','elly@hotmail.com',38439),
(6,'Fred','fs1@ymail.com',29483),
(7,'Gru','gru@aol.com',17281),
(8,'Howard','hh@gmail.com',25024),
(9,'Iris','Iris@ymail.com',33563),
(10,'Jake','jakeh@aol.com',23423),
(11,'Kim','kim@ymail.com',23849),
(12,'Lowel','lm@gmail.com',89562),
(13,'Mason','mason@hotmail.com',17745),
(14,'Nicky','nicky@ymail.com',12459),
(15,'Oliver','ollie@gmail.com',35235)

insert into Dim_Manufacturer values
(1,'Apple'),
(2,'Nokia'),
(3,'Samsung'),
(4,'Mi'),
(5,'Motorola')

insert into Dim_Model values
(101,'Iphone 7 32GB',400,1),
(102,'Iphone 7 128GB',500,1),
(103,'Iphone 7S 32GB',500,1),
(104,'Iphone 7S 128GB',600,1),
(105,'Iphone 7S 256GB',700,1),
(106,'Iphone 8 32GB',400,1),
(107,'Iphone 8 128GB',500,1),
(108,'Iphone 8S 32GB',500,1),
(109,'Iphone 8S 128GB',600,1),
(110,'Iphone 8S 256GB',700,1),
(111,'Nokia X6',200,2),
(112,'Nokia X8',350,2),
(113,'Samsung S8 32GB',300,3),
(114,'Samsung S8 64GB',350,3),
(115,'Samsung S8 128GB',450,3),
(116,'Mi 5',100,4),
(117,'Mi 8',200,4),
(118,'Mi 6',175,4),
(119,'Moto Z',500,5),
(120,'Moto G5',350,5)

insert into Dim_Date values
('12/23/2014'),
('4/29/2015'),
('3/17/2011'),
('6/8/2010'),
('8/12/2016'),
('11/1/2017'),
('10/31/2015'),
('1/15/2012'),
('2/3/2011'),
('5/18/2016'),
('7/6/2017'),
('9/23/2013'),
('5/27/2010'),
('4/18/2009'),
('3/16/2013'),
('7/29/2017'),
('2/14/2011'),
('7/31/2012'),
('4/15/2015'),
('11/23/2011')

insert into Fact_Transaction values
(102,13,9,'4/18/2009',800,1),
(108,4,2,'1/15/2012',1500,4),
(112,9,10,'11/23/2011',1100,3),
(116,11,4,'1/15/2012',700,2),
(101,2,6,'8/12/2016',2000,5),
(115,15,8,'5/27/2010',400,1),
(115,6,1,'3/16/2013',1300,4),
(120,8,12,'7/29/2017',900,2),
(109,3,14,'11/1/2017',1800,5),
(117,10,15,'12/23/2014',1200,4),
(104,6,14,'4/15/2015',500,1),
(115,3,8,'2/14/2011',700,1),
(101,13,6,'7/6/2017',1000,3),
(114,5,11,'6/8/2010',400,1),
(110,12,15,'5/18/2016',1400,3),
(107,6,13,'10/31/2015',800,2),
(111,15,2,'9/23/2013',3000,5),
(110,12,3,'3/17/2011',1150,4),
(118,10,3,'8/12/2016',500,1),
(113,4,5,'7/31/2012',450,1),
(114,9,9,'5/27/2010',2300,5),
(119,13,3,'2/3/2011',230,1),
(113,6,8,'4/29/2015',350,1),
(115,2,8,'2/3/2011',3400,6),
(117,9,5,'4/18/2009',1500,3),
(103,13,10,'10/31/2015',1300,2),
(113,11,7,'3/16/2013',400,1),
(120,6,2,'12/23/2014',1200,3),
(112,14,10,'9/23/2013',1450,4),
(118,9,15,'2/3/2011',350,1)

select Fact_Transaction.IdCustomer, Dim_Customer.Customer_name, [Total price]
from Fact_Transaction left outer join Dim_Customer
on Fact_Transaction.IdCustomer = Dim_Customer.IdCustomer
order by IdCustomer asc




