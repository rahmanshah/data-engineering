
CREATE PROCEDURE dbo.new_customer
    @firstname_in NVARCHAR(50),
    @lastname_in NVARCHAR(50),
    @address1_in NVARCHAR(50),
    @address2_in NVARCHAR(50),
    @city_in NVARCHAR(50),
    @state_in NVARCHAR(50),
    @zip_in INT,
    @country_in NVARCHAR(50),
    @region_in INT,
    @email_in NVARCHAR(50),
    @phone_in NVARCHAR(50),
    @creditcardtype_in INT,
    @creditcard_in NVARCHAR(50),
    @creditcardexpiration_in NVARCHAR(50),
    @username_in NVARCHAR(50),
    @password_in NVARCHAR(50),
    @age_in INT,
    @income_in INT,
    @gender_in NVARCHAR(1),
    @customerid_out INT OUTPUT
AS
BEGIN
    DECLARE @rows_returned INT;
    
    SELECT @rows_returned = COUNT(*) FROM dbo.customers WHERE username = @username_in;
    
    IF @rows_returned = 0 
    BEGIN
        INSERT INTO dbo.customers
        (
            firstname,
            lastname,
            email,
            phone,
            username,
            password,
            address1,
            address2,
            city,
            state,
            zip,
            country,
            region,
            creditcardtype,
            creditcard,
            creditcardexpiration,
            age,
            income,
            gender
        )
        VALUES
        (
            @firstname_in,
            @lastname_in,
            @email_in,
            @phone_in,
            @username_in,
            @password_in,
            @address1_in,
            @address2_in,
            @city_in,
            @state_in,
            @zip_in,
            @country_in,
            @region_in,
            @creditcardtype_in,
            @creditcard_in,
            @creditcardexpiration_in,
            @age_in,
            @income_in,
            @gender_in
        );
        
        SET @customerid_out = SCOPE_IDENTITY();
    END
    ELSE
    BEGIN
        SET @customerid_out = 0;
    END
END;

GO

