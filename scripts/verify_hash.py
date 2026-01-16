import bcrypt

password = "SecurePass123!"
hashed = "$2b$12$7Z4uqtZjtLjcrOpA5h0NsOK1CttU3zqNP.WT2BBSlXbVnLjsSAGcm"

if bcrypt.checkpw(password.encode(), hashed.encode()):
    print("Password Match!")
else:
    print("Password Mismatch!")
