@echo off
git init
git config core.autocrlf false
git config user.name "AI Assistant"
git config user.email "engineering@example.com"
git add .
git commit -m "Initial commit ✨: Full ETL Engine Architecture setup"
git branch -M main
git remote remove origin 2>nul
git remote add origin https://github.com/mohamd764/HealthSync-Engine.git
git push -u origin main
