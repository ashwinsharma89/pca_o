import bcrypt

password = "SecurePass123!"
hashed = "$2b$12$UsV.2PS7tMNKPIrFBIicy.uPjLTQHZJLC0VOBn5hnXZ2LbslFGHjS"

if bcrypt.checkpw(password.encode(), hashed.encode()):
    print("Password Match!")
else:
    print("Password Mismatch!")
