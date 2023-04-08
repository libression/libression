"use strict"

function _renderImpression(file_key){
    var existing_impression = document.getElementById(file_key)
    if (existing_impression) {
        return existing_impression
    } else {
        return _fetchImpression(file_key)
    }
}

function _fetchImpression(file_key){
    var checkbox = document.createElement("input")
    checkbox.className = "selected_photos"
    checkbox.type = "checkbox"
    checkbox.value = file_key

    var img = new Image()
    img.src = window.location.href + "thumbnail/" + file_key

    var figcaption = document.createElement("figcaption")
    var file_key_tokens = file_key.split("/")
    figcaption.innerHTML = file_key_tokens[file_key_tokens.length - 1]

    var outer_impression = document.createElement("a")
    outer_impression.href = window.location.href + "download/" + file_key
    outer_impression.target="popup"
    outer_impression.appendChild(checkbox)
    outer_impression.id = file_key

    var impression = document.createElement("figure")
    impression.id = file_key
    impression.appendChild(img)
    impression.appendChild(figcaption)

    outer_impression.appendChild(impression)

    return outer_impression
}

function refreshGallery(file_keys){
    var new_inner_gallery = document.createElement("div")
    new_inner_gallery.className = "gallery"
    file_keys?.forEach(function (file_key) {
        var impression = _renderImpression(file_key)
        new_inner_gallery.appendChild(impression)
    })

    var gallery = document.getElementsByClassName("gallery")
    if (gallery){
        gallery[0].replaceWith(new_inner_gallery)
    }
}

export { refreshGallery }
