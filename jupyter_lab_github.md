Para trabajar con notebooks de tu repositorio en GitHub en Jupyter Lab de forma fácil y sincronizada, sigue estos pasos:

### 1️⃣ **Clonar el repositorio en local (Solo la primera vez)**
Abre una terminal (CMD, PowerShell o Git Bash) y ejecuta:

```bash
git clone https://github.com/DavidP0011/etl.git
cd etl
```

### 2️⃣ **Abrir el repositorio en Jupyter Lab**
Ejecuta Jupyter Lab en la carpeta del repositorio:

```bash
jupyter lab
```

Desde ahí, podrás abrir y editar los notebooks como cualquier otro archivo.

---

### 3️⃣ **Actualizar el repositorio con cambios**
Después de modificar un notebook, guarda los cambios y actualiza el repositorio con:

```bash
git add .
git commit -m "Actualización de notebooks"
git push origin main  # Reemplaza 'main' si tu rama principal tiene otro nombre
```

Si hay cambios remotos antes de hacer `push`, asegúrate de hacer un `pull` antes:

```bash
git pull origin main --rebase
```

---

### 🚀 **Alternativa rápida sin clonar (Para notebooks individuales)**
Si solo quieres descargar y editar un notebook sin clonar el repo, usa:

```bash
wget https://raw.githubusercontent.com/DavidP0011/etl/main/GBQ_global_info.ipynb
```
Lo editas en Jupyter y luego lo subes manualmente a GitHub.

Si quieres automatizar la subida, necesitarás clonar el repo y hacer `push` como en el método anterior.

---

### 📌 **Extensión útil: Git en Jupyter Lab**
Para facilitar el trabajo con Git dentro de Jupyter Lab, puedes instalar la extensión oficial:

```bash
pip install jupyterlab-git
jupyter lab build
```

Luego, en la interfaz de Jupyter, aparecerá una pestaña de control de versiones para gestionar cambios de forma visual.

---

### 🔥 **Resumen**
- Clona el repo una vez con `git clone`
- Abre y edita en `jupyter lab`
- Guarda cambios con `git add . && git commit -m "mensaje" && git push origin main`
- Usa la extensión `jupyterlab-git` si prefieres una interfaz gráfica

¿Te interesa automatizar la actualización del repo con algún script? 🚀
