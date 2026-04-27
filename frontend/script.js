function handleFile(input) {
    const file = input.files[0];
    const fileSelected = document.getElementById("fileSelected");
    const fileName = document.getElementById("fileName");
    const fileSize = document.getElementById("fileSize");
    
    if (file) {
        fileSelected.classList.add("show");
        fileName.textContent = file.name;
        if (fileSize) {
            fileSize.textContent = `${(file.size / 1024).toFixed(1)} KB`;
        }
    }
}

document.addEventListener("DOMContentLoaded", () => {
    const statusEl = document.getElementById("status");
    const btn = document.querySelector("#tab-cv .submit-btn");

    console.log("DOM ready");
    console.log("btn found:", btn);

    if (!btn) {
        console.error("BOUTON INTROUVABLE");
        return;
    }

    btn.addEventListener("click", async (e) => {
        e.preventDefault();
        console.log("CLICK détecté");

        const file = document.getElementById("fileInput").files[0];
        if (!file) {
            statusEl.innerText = "Veuillez sélectionner un fichier.";
            return;
        }

        console.log("Fichier:", file.name);
        statusEl.innerText = "Upload en cours...";

        const formData = new FormData();
        formData.append("file", file);

        try {
            // Utilisez l'URL relative ou l'URL complète selon la configuration
            const apiUrl = window.location.hostname === 'localhost' 
                ? 'http://localhost:5000/upload'
                : '/upload';
            
            const response = await fetch(apiUrl, {
                method: "POST",
                body: formData
            });
            
            const data = await response.json();
            
            if (response.ok) {
                statusEl.innerHTML = "✅ CV uploadé avec succès ! Analyse en cours...";
                // Appeler l'analyse du CV
                await analyzeCV(file.name);
            } else {
                statusEl.innerHTML = `❌ Erreur: ${data.error || "Erreur serveur"}`;
            }
        } catch (error) {
            statusEl.innerHTML = "❌ Serveur inaccessible. Vérifiez que le backend est démarré.";
            console.error("Erreur:", error);
        }
    });
});

async function analyzeCV(filename) {
    try {
        const response = await fetch('http://localhost:5000/analyze', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ filename: filename })
        });
        const data = await response.json();
        console.log("Analyse résultats:", data);
    } catch (error) {
        console.error("Erreur analyse:", error);
    }
}

// Fonctions pour l'onglet texte
function switchTab(tab, element) {
    // Mettre à jour les onglets
    document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
    element.classList.add('active');
    
    // Mettre à jour le contenu
    document.getElementById('tab-cv').classList.remove('active');
    document.getElementById('tab-text').classList.remove('active');
    document.getElementById(`tab-${tab}`).classList.add('active');
}

function updateCount(textarea) {
    const count = textarea.value.length;
    document.getElementById('charCount').textContent = count;
}

function addTag(element) {
    const textarea = document.getElementById('profileText');
    const tagText = element.textContent;
    const currentText = textarea.value;
    
    if (currentText) {
        textarea.value = currentText + ' ' + tagText;
    } else {
        textarea.value = tagText;
    }
    updateCount(textarea);
}

async function submitText() {
    const profileText = document.getElementById('profileText').value;
    const statusEl = document.getElementById('statusText');
    
    if (!profileText.trim()) {
        statusEl.innerHTML = "❌ Veuillez décrire votre profil";
        return;
    }
    
    statusEl.innerHTML = "Analyse en cours...";
    
    try {
        const response = await fetch('http://localhost:5000/analyze-text', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ text: profileText })
        });
        
        const data = await response.json();
        
        if (response.ok) {
            statusEl.innerHTML = "✅ Profil analysé avec succès !";
            // Afficher les résultats
            displayResults(data);
        } else {
            statusEl.innerHTML = `❌ Erreur: ${data.error}`;
        }
    } catch (error) {
        statusEl.innerHTML = "❌ Erreur de connexion au serveur";
        console.error(error);
    }
}

function displayResults(data) {
    // Créer ou afficher une section de résultats
    let resultsDiv = document.getElementById('results');
    if (!resultsDiv) {
        resultsDiv = document.createElement('div');
        resultsDiv.id = 'results';
        resultsDiv.className = 'results-section';
        document.querySelector('.main-card').after(resultsDiv);
    }
    
    resultsDiv.innerHTML = `
        <div class="results-card">
            <h3>Compétences détectées</h3>
            <div class="skills-list">
                ${data.skills.map(skill => `<span class="skill-tag">${skill}</span>`).join('')}
            </div>
            <h3>Offres recommandées</h3>
            <div class="jobs-list">
                ${data.jobs ? data.jobs.map(job => `
                    <div class="job-card">
                        <h4>${job.title}</h4>
                        <p>${job.description}</p>
                        <span class="match-score">Match: ${job.score}%</span>
                    </div>
                `).join('') : '<p>Aucune offre trouvée</p>'}
            </div>
        </div>
    `;
}

function openDashboard() {
    window.open('http://localhost:5000/dashboard', '_blank');
}

// Drag & drop pour l'upload
const uploadZone = document.querySelector('.upload-zone');
if (uploadZone) {
    uploadZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        uploadZone.classList.add('drag-over');
    });
    
    uploadZone.addEventListener('dragleave', () => {
        uploadZone.classList.remove('drag-over');
    });
    
    uploadZone.addEventListener('drop', (e) => {
        e.preventDefault();
        uploadZone.classList.remove('drag-over');
        const file = e.dataTransfer.files[0];
        if (file && file.type === 'application/pdf') {
            const input = document.getElementById('fileInput');
            input.files = e.dataTransfer.files;
            handleFile(input);
        }
    });
}