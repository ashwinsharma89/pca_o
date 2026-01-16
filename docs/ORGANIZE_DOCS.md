# 📋 Documentation Organization Guide

This guide shows how to organize existing documentation files into the new `docs/` folder structure.

---

## 📁 **Current Documentation Files**

### **Files to Move**:

```
Root Directory Files:
├── ARCHITECTURE.md                    → docs/architecture/
├── PREDICTIVE_ANALYTICS_ARCHITECTURE.md → docs/architecture/
├── ANALYSIS_FRAMEWORK.md              → docs/architecture/
├── PREDICTIVE_IMPLEMENTATION_GUIDE.md → docs/user-guides/
├── PREDICTIVE_QUICKSTART.md           → docs/user-guides/
├── DASHBOARD_USER_GUIDE.md            → docs/user-guides/
├── TEST_RESULTS_SUMMARY.md            → docs/development/
├── PROJECT_STATUS.md                  → docs/planning/
├── DOCUMENTATION_PLAN.md              → docs/planning/
├── DEPLOYMENT.md                      → docs/development/
└── README.md                          → Keep in root (main README)
```

---

## 🎯 **Recommended Organization**

### **1. Architecture Documentation** → `docs/architecture/`

Move these files:
- `ARCHITECTURE.md`
- `PREDICTIVE_ANALYTICS_ARCHITECTURE.md`
- `ANALYSIS_FRAMEWORK.md`

**Command**:
```bash
move ARCHITECTURE.md docs\architecture\
move PREDICTIVE_ANALYTICS_ARCHITECTURE.md docs\architecture\
move ANALYSIS_FRAMEWORK.md docs\architecture\
```

---

### **2. User Guides** → `docs/user-guides/`

Move these files:
- `PREDICTIVE_IMPLEMENTATION_GUIDE.md`
- `PREDICTIVE_QUICKSTART.md`
- `DASHBOARD_USER_GUIDE.md`

**Command**:
```bash
move PREDICTIVE_IMPLEMENTATION_GUIDE.md docs\user-guides\
move PREDICTIVE_QUICKSTART.md docs\user-guides\
move DASHBOARD_USER_GUIDE.md docs\user-guides\
```

---

### **3. Development Documentation** → `docs/development/`

Move these files:
- `TEST_RESULTS_SUMMARY.md`
- `DEPLOYMENT.md`

**Command**:
```bash
move TEST_RESULTS_SUMMARY.md docs\development\
move DEPLOYMENT.md docs\development\
```

---

### **4. Planning Documentation** → `docs/planning/`

Move these files:
- `PROJECT_STATUS.md`
- `DOCUMENTATION_PLAN.md`

**Command**:
```bash
move PROJECT_STATUS.md docs\planning\
move DOCUMENTATION_PLAN.md docs\planning\
```

---

## 🔄 **Migration Script**

### **Windows PowerShell**:

```powershell
# Navigate to project root
cd "C:\Users\asharm08\OneDrive - dentsu\Desktop\windsurf\PCA_Agent"

# Move architecture docs
Move-Item -Path "ARCHITECTURE.md" -Destination "docs\architecture\" -ErrorAction SilentlyContinue
Move-Item -Path "PREDICTIVE_ANALYTICS_ARCHITECTURE.md" -Destination "docs\architecture\" -ErrorAction SilentlyContinue
Move-Item -Path "ANALYSIS_FRAMEWORK.md" -Destination "docs\architecture\" -ErrorAction SilentlyContinue

# Move user guides
Move-Item -Path "PREDICTIVE_IMPLEMENTATION_GUIDE.md" -Destination "docs\user-guides\" -ErrorAction SilentlyContinue
Move-Item -Path "PREDICTIVE_QUICKSTART.md" -Destination "docs\user-guides\" -ErrorAction SilentlyContinue
Move-Item -Path "DASHBOARD_USER_GUIDE.md" -Destination "docs\user-guides\" -ErrorAction SilentlyContinue

# Move development docs
Move-Item -Path "TEST_RESULTS_SUMMARY.md" -Destination "docs\development\" -ErrorAction SilentlyContinue
Move-Item -Path "DEPLOYMENT.md" -Destination "docs\development\" -ErrorAction SilentlyContinue

# Move planning docs
Move-Item -Path "PROJECT_STATUS.md" -Destination "docs\planning\" -ErrorAction SilentlyContinue
Move-Item -Path "DOCUMENTATION_PLAN.md" -Destination "docs\planning\" -ErrorAction SilentlyContinue

Write-Host "✅ Documentation files organized successfully!"
```

---

## 📝 **Update Links**

After moving files, update links in other documents:

### **In README.md** (root):
```markdown
# Old links
[Architecture](ARCHITECTURE.md)

# New links
[Architecture](docs/architecture/ARCHITECTURE.md)
```

### **In Code Files**:
```python
# Old path
with open('PREDICTIVE_QUICKSTART.md', 'r') as f:

# New path
with open('docs/user-guides/PREDICTIVE_QUICKSTART.md', 'r') as f:
```

---

## ✅ **Verification Checklist**

After organizing:

- [ ] All files moved to correct folders
- [ ] Links updated in README.md
- [ ] Links updated in other docs
- [ ] Code references updated
- [ ] Test that all links work
- [ ] Verify no broken references

---

## 🎯 **Final Structure**

After organization, you should have:

```
PCA_Agent/
├── README.md                          # Main README (stays in root)
├── docs/                              # All documentation here
│   ├── README.md                      # Documentation index
│   ├── architecture/
│   │   ├── ARCHITECTURE.md
│   │   ├── PREDICTIVE_ANALYTICS_ARCHITECTURE.md
│   │   └── ANALYSIS_FRAMEWORK.md
│   ├── user-guides/
│   │   ├── PREDICTIVE_IMPLEMENTATION_GUIDE.md
│   │   ├── PREDICTIVE_QUICKSTART.md
│   │   └── DASHBOARD_USER_GUIDE.md
│   ├── development/
│   │   ├── TEST_RESULTS_SUMMARY.md
│   │   └── DEPLOYMENT.md
│   └── planning/
│       ├── PROJECT_STATUS.md
│       └── DOCUMENTATION_PLAN.md
└── ... (other project files)
```

---

## 💡 **Tips**

1. **Backup First**: Make a backup before moving files
2. **Test Links**: Test all documentation links after moving
3. **Update Gradually**: Move files in batches and test
4. **Keep README**: Main README.md stays in root
5. **Update References**: Update all code/doc references

---

## 🚀 **Quick Commands**

### **Move All at Once** (PowerShell):
```powershell
# Run from project root
.\organize_docs.ps1
```

### **Verify Organization**:
```powershell
# Check docs folder structure
tree docs /F
```

---

**Ready to organize? Run the migration script above!** 📚✨
