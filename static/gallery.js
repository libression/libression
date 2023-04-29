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

    var img = document.createElement("img")
    img.className="w3-hover-opacity"
    img.src = window.location.href + "thumbnail/" + file_key
    img.alt = file_key
    img.style = "width:100%"
    
    var impression = document.createElement("a")
    impression.href = window.location.href + "download/" + file_key
    impression.target="popup"
    impression.appendChild(img)

    var tooltip = document.createElement("span")
    tooltip.style = "position:absolute;left:0;bottom:18px"
    tooltip.className = "w3-text w3-tag w3-tiny w3-animate-opacity"
    tooltip.innerText = file_key

    var checkbox = document.createElement("input")
    checkbox.className = "selected_photos w3-check"
    checkbox.type = "checkbox"
    checkbox.value = file_key

    var outer_impression = document.createElement("a")
    outer_impression.className = "w3-quarter w3-container w3-tooltip"
    outer_impression.id = file_key

    outer_impression.appendChild(checkbox)
    outer_impression.append(tooltip)
    outer_impression.appendChild(impression)

    return outer_impression
}

function refreshGallery(file_keys){
    var new_inner_gallery = document.createElement("div")
    new_inner_gallery.className = "w3-row w3-border"
    new_inner_gallery.id = "gallery"
    file_keys?.forEach(function (file_key) {
        var impression = _renderImpression(file_key)
        new_inner_gallery.appendChild(impression)
    })

    var gallery = document.getElementById("gallery")
    if (gallery){
        gallery.replaceWith(new_inner_gallery)
    }
}

export { refreshGallery }
