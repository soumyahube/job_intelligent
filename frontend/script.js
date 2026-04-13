const form = document.getElementById("uploadForm");
const statusText = document.getElementById("status");

form.addEventListener("submit", async (e) => {
    e.preventDefault();

    const fileInput = document.getElementById("fileInput");
    const file = fileInput.files[0];

    if (!file) {
        statusText.innerText = "Veuillez sélectionner un fichier.";
        return;
    }

    const formData = new FormData();
    formData.append("file", file);

    statusText.innerText = "Upload en cours...";

    try {
        console.log("START FETCH");

        const response = await fetch("http://127.0.0.1:5000/upload", {
            method: "POST",
            body: formData
        });

        console.log("END FETCH");

        const data = await response.json();

        if (response.ok) {
            statusText.innerText = "CV uploadé avec succès ";
            console.log(data);
        } else {
            statusText.innerText = " Erreur serveur.";
        }

    } catch (error) {
        statusText.innerText = " Serveur inaccessible.";
        console.error(error);
    }
});