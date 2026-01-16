from setuptools import setup, find_packages

setup(
    name="pca_agent",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[],
)
```

### Step 2: Update `requirements.txt`

Add this line at the **top** of your `requirements.txt` file:
```
-e .
