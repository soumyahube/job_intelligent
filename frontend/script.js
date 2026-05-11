// ─────────────────────────────────────────────
// CONFIG
// ─────────────────────────────────────────────
const API_BASE = window.location.hostname === "localhost"
  ? "http://localhost:5000"
  : "";

// ─────────────────────────────────────────────
// CURSEUR PERSONNALISÉ
// ─────────────────────────────────────────────
const cursor     = document.getElementById("cursor");
const cursorRing = document.getElementById("cursorRing");
if (cursor && cursorRing) {
  document.addEventListener("mousemove", e => {
    cursor.style.left     = e.clientX + "px";
    cursor.style.top      = e.clientY + "px";
    cursorRing.style.left = e.clientX + "px";
    cursorRing.style.top  = e.clientY + "px";
  });
}

// ─────────────────────────────────────────────
// ONGLETS
// ─────────────────────────────────────────────
function switchTab(tab, btn) {
  document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
  document.querySelectorAll(".tab-content").forEach(t => t.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById("tab-" + tab).classList.add("active");
  hideRecommendations();
}

// ─────────────────────────────────────────────
// SÉLECTION DE FICHIER
// ─────────────────────────────────────────────
function handleFile(input) {
  const file         = input.files[0];
  const fileSelected = document.getElementById("fileSelected");
  const fileName     = document.getElementById("fileName");
  const fileSize     = document.getElementById("fileSize");
  if (file) {
    fileSelected.classList.add("show");
    fileName.textContent = file.name;
    if (fileSize) fileSize.textContent = `${(file.size / 1024).toFixed(1)} KB`;
  }
}

function updateCount(el) {
  const counter = document.getElementById("charCount");
  if (counter) counter.textContent = el.value.length;
}

function addTag(el) {
  const textarea = document.getElementById("profileText");
  if (!textarea) return;
  const tag = el.textContent.trim();
  textarea.value += (textarea.value ? ", " : "") + tag;
  updateCount(textarea);
}

// ─────────────────────────────────────────────
// UPLOAD CV → /upload → polling → /recommend
// ─────────────────────────────────────────────
document.addEventListener("DOMContentLoaded", () => {
  const btn = document.querySelector("#tab-cv .submit-btn");
  if (!btn) return;

  btn.addEventListener("click", async (e) => {
    e.preventDefault();
    const file     = document.getElementById("fileInput").files[0];
    const statusEl = document.getElementById("status");

    if (!file) {
      statusEl.innerText = "⚠️ Veuillez sélectionner un fichier PDF.";
      return;
    }

    setStatus("status", "⏳ Upload en cours...", "loading");

    const formData = new FormData();
    formData.append("file", file);

    try {
      const res  = await fetch(`${API_BASE}/upload`, { method: "POST", body: formData });
      const data = await res.json();

      if (!res.ok) {
        setStatus("status", `❌ Erreur : ${data.error || "Upload échoué"}`, "error");
        return;
      }

      setStatus("status", "✅ CV uploadé ! Analyse IA en cours…", "loading");

      // Attendre que le DAG Airflow génère l'embedding (polling)
      const candidateId = await pollForCandidate(15, 4000);
      if (!candidateId) {
        setStatus("status", "⏳ Analyse toujours en cours… revenez dans quelques secondes.", "warning");
        showRetryButton(null);
        return;
      }

      setStatus("status", "🔍 Calcul des recommandations…", "loading");
      await fetchAndDisplayRecommendations(candidateId, "status");

    } catch (err) {
      setStatus("status", `❌ Erreur réseau : ${err.message}`, "error");
    }
  });
});

// ─────────────────────────────────────────────
// TEXTE LIBRE → /analyze-text → polling → /recommend
// ─────────────────────────────────────────────
async function submitText() {
  const text     = document.getElementById("profileText").value.trim();
  const statusEl = document.getElementById("statusText");

  if (!text) {
    setStatus("statusText", "⚠️ Veuillez décrire votre profil.", "warning");
    return;
  }

  setStatus("statusText", "⏳ Analyse en cours…", "loading");

  try {
    const res  = await fetch(`${API_BASE}/analyze-text`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ text }),
    });
    const data = await res.json();

    if (!res.ok) {
      setStatus("statusText", `❌ Erreur : ${data.error}`, "error");
      return;
    }

    setStatus("statusText", "✅ Profil soumis ! Analyse IA en cours…", "loading");

    const candidateId = await pollForCandidate(15, 4000);
    if (!candidateId) {
      setStatus("statusText", "⏳ Analyse toujours en cours… revenez dans quelques secondes.", "warning");
      return;
    }

    setStatus("statusText", "🔍 Calcul des recommandations…", "loading");
    await fetchAndDisplayRecommendations(candidateId, "statusText");

  } catch (err) {
    setStatus("statusText", `❌ Erreur réseau : ${err.message}`, "error");
  }
}

// ─────────────────────────────────────────────
// POLLING — attend que le DAG Airflow finisse
// Essaie toutes les `interval` ms, jusqu'à `maxTries`
// ─────────────────────────────────────────────
async function pollForCandidate(maxTries = 15, interval = 4000) {
  for (let i = 0; i < maxTries; i++) {
    await sleep(interval);
    try {
      const res  = await fetch(`${API_BASE}/candidate/latest`);
      const data = await res.json();
      if (res.ok && data.candidate_id) return data.candidate_id;
    } catch (_) {}
  }
  return null;
}

// ─────────────────────────────────────────────
// FETCH RECOMMANDATIONS
// ─────────────────────────────────────────────
async function fetchAndDisplayRecommendations(candidateId, statusElId) {
  try {
    const res  = await fetch(`${API_BASE}/recommend/${candidateId}?top=10`);
    const data = await res.json();

    if (!res.ok) {
      setStatus(statusElId, `❌ ${data.error}`, "error");
      return;
    }

    setStatus(statusElId, `✅ ${data.total} offres trouvées pour vous !`, "success");
    renderRecommendations(data.recommendations);

  } catch (err) {
    setStatus(statusElId, `❌ Erreur : ${err.message}`, "error");
  }
}

// ─────────────────────────────────────────────
// RENDU DES RECOMMANDATIONS
// ─────────────────────────────────────────────
function renderRecommendations(offers) {
  let section = document.getElementById("recommendations-section");

  // Créer la section si elle n'existe pas encore
  if (!section) {
    section = document.createElement("section");
    section.id = "recommendations-section";
    section.className = "recommendations-section";
    // Insérer après la section hero
    const hero = document.getElementById("search");
    hero.parentNode.insertBefore(section, hero.nextSibling);
  }

  if (!offers || offers.length === 0) {
    section.innerHTML = `
      <div class="section-label">Recommandations IA</div>
      <h2 class="section-title">Aucune offre trouvée</h2>
      <p class="section-sub">Essayez d'enrichir votre CV ou votre description de profil.</p>
    `;
    section.style.display = "block";
    section.scrollIntoView({ behavior: "smooth" });
    return;
  }

  section.innerHTML = `
    <div class="section-label">Recommandations IA</div>
    <h2 class="section-title">Vos meilleures<br><span class="line-accent">opportunités</span></h2>
    <p class="section-sub">Classées par score de compatibilité avec votre profil.</p>
    <div class="reco-grid" id="recoGrid"></div>
  `;

  const grid = section.querySelector("#recoGrid");

  offers.forEach((offer, index) => {
    const score      = Math.round(parseFloat(offer.score) * 100);
    const scoreColor = score >= 75 ? "#4ade80" : score >= 50 ? "#facc15" : "#f87171";
    const salaire    = offer.salaire_min && offer.salaire_max
      ? `${Math.round(offer.salaire_min / 1000)}k–${Math.round(offer.salaire_max / 1000)}k ${offer.devise || ""}`
      : offer.salaire_min
        ? `${Math.round(offer.salaire_min / 1000)}k+ ${offer.devise || ""}`
        : "Salaire non précisé";

    const card = document.createElement("div");
    card.className = "reco-card";
    card.style.animationDelay = `${index * 0.07}s`;
    card.innerHTML = `
      <div class="reco-header">
        <div class="reco-score-ring" style="--score-color:${scoreColor}">
          <span class="reco-score-val">${score}%</span>
          <span class="reco-score-lbl">match</span>
        </div>
        <div class="reco-meta">
          <div class="reco-titre">${offer.titre || "Poste non précisé"}</div>
          <div class="reco-entreprise">${offer.entreprise || "Entreprise non précisée"}</div>
        </div>
      </div>
      <div class="reco-tags">
        ${offer.localisation ? `<span class="reco-tag">📍 ${truncate(offer.localisation, 30)}</span>` : ""}
        ${offer.type_contrat ? `<span class="reco-tag">📋 ${offer.type_contrat}</span>` : ""}
        <span class="reco-tag">💶 ${salaire}</span>
        ${offer.date_publication ? `<span class="reco-tag">🗓 ${offer.date_publication}</span>` : ""}
      </div>
      ${offer.url
        ? `<a href="${offer.url}" target="_blank" rel="noopener" class="reco-btn">Voir l'offre →</a>`
        : `<span class="reco-btn reco-btn-disabled">Lien non disponible</span>`
      }
    `;
    grid.appendChild(card);
  });

  section.style.display = "block";
  section.scrollIntoView({ behavior: "smooth" });
}

function hideRecommendations() {
  const section = document.getElementById("recommendations-section");
  if (section) section.style.display = "none";
}

// ─────────────────────────────────────────────
// HELPERS
// ─────────────────────────────────────────────
function setStatus(elId, msg, type = "") {
  const el = document.getElementById(elId);
  if (!el) return;
  el.innerText = msg;
  el.className = type ? `status-msg status-${type}` : "status-msg";
}

function truncate(str, max) {
  return str && str.length > max ? str.slice(0, max) + "…" : str;
}

function sleep(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function openDashboard() {
  alert("Dashboard Power BI — à configurer avec votre lien de publication.");
}

// Stats animées au scroll
function animateCount(el, target, duration = 1500) {
  let start = 0;
  const step = target / (duration / 16);
  const timer = setInterval(() => {
    start += step;
    if (start >= target) { start = target; clearInterval(timer); }
    el.textContent = Math.round(start).toLocaleString("fr-FR");
  }, 16);
}

const observer = new IntersectionObserver(entries => {
  entries.forEach(entry => {
    if (entry.isIntersecting) {
      const el = document.getElementById("countTotal");
      if (el) animateCount(el, 4327);
      observer.disconnect();
    }
  });
}, { threshold: 0.3 });

const statsSection = document.querySelector(".stats-section");
if (statsSection) observer.observe(statsSection);