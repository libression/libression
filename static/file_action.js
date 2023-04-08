"use strict"

async function fileAction(action){
    var photo_checkboxes = document.getElementsByClassName("selected_photos")

    var selected_keys = []

    for (var i = 0; i < photo_checkboxes.length; i++) {
        var photo_element = photo_checkboxes[i]
        if (photo_element.checked) {
            selected_keys.push(photo_element.value)
            photo_element.checked = false  // not sure if need to?
        }
    }
    
    var target_dir = document.getElementById("target_dir_input").value

    var url = window.location.href + "file_action"
    var payload = {
        "action": action,
        "file_keys": selected_keys,
        "target_dir": target_dir
    }

    await fetch(
        url, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload), // body data type must match "Content-Type" header
        }
    ).then(
        (response) => response.json()
    )
}

export { fileAction }
