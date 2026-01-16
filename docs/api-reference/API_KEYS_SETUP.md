# 🔑 API Keys Setup Guide

## ✅ System Now Supports Both OpenAI and Claude Sonnet!

The PCA Agent has been updated to support **both OpenAI and Anthropic Claude** as LLM providers.

---

## 🎯 Current Configuration

**Default LLM**: Claude 3.5 Sonnet (Anthropic)
**Model**: `claude-3-5-sonnet-20241022`

The system is configured in `.env` file:
```env
USE_ANTHROPIC=true
DEFAULT_LLM_MODEL=claude-3-5-sonnet-20241022
```

---

## 📝 How to Add Your API Keys

### **Step 1: Open `.env` File**
Location: `c:\Users\asharm08\OneDrive - dentsu\Desktop\windsurf\PCA_Agent\.env`

### **Step 2: Add Your Keys**

Replace the placeholders with your actual API keys:

```env
# API Keys - REPLACE WITH YOUR ACTUAL KEYS
OPENAI_API_KEY=sk-proj-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
ANTHROPIC_API_KEY=sk-ant-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### **Step 3: Save the File**

That's it! The system will automatically use the configured LLM.

---

## 🔄 Switching Between Providers

### **Use Claude Sonnet (Current Default)**
```env
USE_ANTHROPIC=true
DEFAULT_LLM_MODEL=claude-3-5-sonnet-20241022
ANTHROPIC_API_KEY=sk-ant-your-key-here
```

### **Use OpenAI GPT-4**
```env
USE_ANTHROPIC=false
DEFAULT_LLM_MODEL=gpt-4
OPENAI_API_KEY=sk-proj-your-key-here
```

### **Use OpenAI GPT-4 Turbo**
```env
USE_ANTHROPIC=false
DEFAULT_LLM_MODEL=gpt-4-turbo-preview
OPENAI_API_KEY=sk-proj-your-key-here
```

---

## 🎯 Where to Get API Keys

### **Anthropic Claude API Key**
1. Go to: https://console.anthropic.com/
2. Sign up or log in
3. Navigate to "API Keys"
4. Click "Create Key"
5. Copy the key (starts with `sk-ant-`)

### **OpenAI API Key**
1. Go to: https://platform.openai.com/api-keys
2. Sign up or log in
3. Click "Create new secret key"
4. Copy the key (starts with `sk-proj-` or `sk-`)

---

## ✅ What's Been Updated

### **1. Dual LLM Support**
- ✅ OpenAI GPT-4 / GPT-4 Turbo
- ✅ Anthropic Claude 3.5 Sonnet
- ✅ Automatic provider selection
- ✅ Unified API interface

### **2. Updated Files**
- ✅ `src/analytics/auto_insights.py` - Added dual LLM support
- ✅ `.env` - Configured for Claude Sonnet
- ✅ `requirements.txt` - Added anthropic package

### **3. New Method**
```python
def _call_llm(self, system_prompt: str, user_prompt: str, max_tokens: int = 2000) -> str:
    """Call LLM (OpenAI or Anthropic) with unified interface."""
    if self.use_anthropic:
        # Use Claude
        response = self.client.messages.create(...)
    else:
        # Use OpenAI
        response = self.client.chat.completions.create(...)
```

---

## 🚀 How It Works

### **Initialization**
```python
from src.analytics import MediaAnalyticsExpert

# Automatically uses configured LLM from .env
expert = MediaAnalyticsExpert()

# Or explicitly specify
expert = MediaAnalyticsExpert(use_anthropic=True)  # Use Claude
expert = MediaAnalyticsExpert(use_anthropic=False) # Use OpenAI
```

### **Analysis**
```python
# Works the same regardless of LLM provider
analysis = expert.analyze_all(your_dataframe)

# Returns same structure:
# - insights
# - recommendations
# - funnel_analysis
# - roas_analysis
# - etc.
```

---

## 💡 Why Claude Sonnet?

**Advantages:**
- ✅ **Longer Context**: 200K tokens vs GPT-4's 128K
- ✅ **Better Reasoning**: Excellent for complex analysis
- ✅ **Cost Effective**: Lower cost per token
- ✅ **Fast**: Quick response times
- ✅ **Accurate**: High-quality outputs

**Perfect for:**
- Campaign analysis
- Strategic recommendations
- Complex data interpretation
- Multi-dimensional insights

---

## 📊 Model Comparison

| Feature | Claude 3.5 Sonnet | GPT-4 Turbo |
|---------|-------------------|-------------|
| Context Window | 200K tokens | 128K tokens |
| Speed | Fast | Fast |
| Cost (Input) | $3/1M tokens | $10/1M tokens |
| Cost (Output) | $15/1M tokens | $30/1M tokens |
| Reasoning | Excellent | Excellent |
| JSON Output | Excellent | Excellent |

---

## 🔧 Troubleshooting

### **Error: API Key Not Found**
```
ValueError: ANTHROPIC_API_KEY not found
```
**Solution**: Add your Anthropic API key to `.env` file

### **Error: Invalid API Key**
```
AuthenticationError: Invalid API key
```
**Solution**: Check that your API key is correct and active

### **Want to Use OpenAI Instead?**
Set in `.env`:
```env
USE_ANTHROPIC=false
OPENAI_API_KEY=sk-proj-your-key-here
```

---

## ✅ Quick Setup Checklist

- [ ] Open `.env` file
- [ ] Add ANTHROPIC_API_KEY (or OPENAI_API_KEY)
- [ ] Verify USE_ANTHROPIC setting (true for Claude, false for OpenAI)
- [ ] Save file
- [ ] Restart Streamlit app
- [ ] Test with sample data

---

## 🎉 Ready to Use!

Once you add your API key:
1. Restart Streamlit: `streamlit run streamlit_app.py`
2. Upload CSV data
3. Click "Analyze Data & Generate Insights"
4. Get AI-powered analysis using Claude Sonnet!

**The system will automatically use the configured LLM provider!** 🚀

---

## 📞 Need Help?

**Common Issues:**
1. **No API key**: Add key to `.env` file
2. **Wrong provider**: Check USE_ANTHROPIC setting
3. **Rate limits**: Wait a moment and try again
4. **Invalid key**: Verify key is correct and active

**Files to Check:**
- `.env` - API keys and configuration
- `src/analytics/auto_insights.py` - LLM integration
- `requirements.txt` - Package dependencies

---

**Both OpenAI and Claude Sonnet are now fully supported!** Choose the one that works best for you! 🎯
