"use strict"
import { fileAction } from './file_action.js'
import { refreshGallery } from './gallery.js'


function _refreshInnerDirs(inner_dirs){

    var hover_div = document.createElement("div")
    hover_div.className = "w3-dropdown-hover"

    var hover_button = document.createElement("button")
    hover_button.className = "w3-button"
    hover_button.innerText = "/..."

    hover_div.appendChild(hover_button)

    var dropdown_content = document.createElement("div")
    dropdown_content.className = "w3-dropdown-content w3-bar-block w3-card-4"
    inner_dirs?.forEach(function (inner_dir) {
        var folder_name = inner_dir.split("/").at(-1)
        var inner_dir_a = _newPathButton(inner_dir, folder_name)
        dropdown_content.appendChild(inner_dir_a)
    })

    hover_div.append(dropdown_content)

    return hover_div
}


function _newPathButton(target_path, show_text){
    var dom = document.createElement("a")
    dom.className = "w3-bar-item w3-button"
    dom.innerText = show_text
    dom.addEventListener('click', function(){
        refreshPage(target_path)
    })
    return dom
}


function updateNavBar(cur_dir, inner_dirs){
    var dom = document.getElementById("navigation_bar")
    var new_dom = document.createElement("div")
    new_dom.class = "w3-bar"
    new_dom.style = "background-color:#39ac73"
    new_dom.id = "navigation_bar"

    new_dom.append(_createHome())

    var cur_dir_tokens = cur_dir.split("/")

    if (cur_dir_tokens.at(0) != ""){
        for (var i = 0; i < cur_dir_tokens.length; i++) {
            var target_path = cur_dir_tokens.slice(0, i+1).join("/")
            var show_text = "/".concat(cur_dir_tokens[i])
            var partial_path = _newPathButton(target_path, show_text)
            new_dom.appendChild(partial_path)
        }
    }

    var inner_dir_a = _refreshInnerDirs(inner_dirs)
    inner_dir_a.id = "cur_dir"
    inner_dir_a.value = cur_dir

    new_dom.append(inner_dir_a)

    dom.replaceWith(new_dom)  // clears old listeners
}


function _createHome(){
    var home = document.createElement("i")
    home.className = "w3-bar-item w3-button fa fa-home"
    home.addEventListener('click', goHome)
    return home
}


function _refreshCheckboxes(query_str, cur_dir){
    var dom = document.querySelector(query_str)
    var new_dom = dom.cloneNode(true)
    new_dom.addEventListener('click', function(){
        refreshPage(cur_dir)
    })
    dom.replaceWith(new_dom)  // clears old listeners
}

function _updateDOM(response_dict, cur_dir){
    // Update DOM
    updateNavBar(cur_dir, response_dict["inner_dirs"])
    refreshGallery(response_dict["file_keys"])
}


async function refreshPage(cur_dir){
    var show_subdirs = document.querySelector("#show_subdirs").checked
    var show_hidden_content = document.querySelector("#show_hidden_content").checked

    var url = window.location.href + "refresh_page_params"
    var payload = {
        "cur_dir": cur_dir,
        "show_subdirs": show_subdirs,
        "show_hidden_content": show_hidden_content
    }

    _refreshCheckboxes("#show_subdirs", cur_dir)
    _refreshCheckboxes("#show_hidden_content", cur_dir)

    await fetch(
        url, {
            method: "POST",
            headers: {"Content-Type": "application/json"},
            body: JSON.stringify(payload), // body data type must match "Content-Type" header
        }
    ).then(
        (response) => response.json()
    ).then(
        (data) => _updateDOM(data, cur_dir)
    )
}

async function copyKeys(){
    await fileAction("copy")
    var cur_dir = document.querySelector("#cur_dir").value
    refreshPage(cur_dir)
}

async function moveKeys(){
    await fileAction("move")
    var cur_dir = document.querySelector("#cur_dir").value
    refreshPage(cur_dir)
}

async function deleteKeys(){
    await fileAction("delete")
    var cur_dir = document.querySelector("#cur_dir").value
    refreshPage(cur_dir)
}

async function goHome(){await refreshPage("")}

window.addEventListener('load', goHome())

const copy_action = document.getElementById("copy_action");
copy_action.addEventListener('click', copyKeys)

const move_action = document.getElementById("move_action");
move_action.addEventListener('click', moveKeys)

const delete_action = document.getElementById("delete_action");
delete_action.addEventListener('click', deleteKeys)
