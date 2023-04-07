"use strict"
import { fileAction } from './file_action.js'
import { refreshGallery } from './gallery.js'


function _updateDirListener(dom, dir_str, prefix=""){
    dom.innerText = prefix.concat(dir_str)
    dom.addEventListener('click', function(){
        refreshPage(dir_str)
    })
    return dom
}

function _refreshInnerDirs(inner_dirs){
    var new_inner_dirs = document.createElement("div")
    new_inner_dirs.className = "dropdown-content"
    inner_dirs?.forEach(function (inner_dir) {
        var inner_dir_elem = document.createElement("a")
        _updateDirListener(inner_dir_elem, inner_dir, "/")
        new_inner_dirs.appendChild(inner_dir_elem)
    })

    var inner_dir_dom = document.getElementsByClassName("dropdown-content")
    if (inner_dir_dom){
        inner_dir_dom[0].replaceWith(new_inner_dirs)
    }
}

function _refreshOuterDirs(outer_dir){
    var dom = document.querySelector("#outer_dir")
    var new_dom = dom.cloneNode(true)
    _updateDirListener(new_dom, outer_dir, "par_dir: /")
    dom.replaceWith(new_dom)  // clears old listeners
}

function _refreshCheckboxes(query_str, cur_dir){
    var dom = document.querySelector(query_str)
    var new_dom = dom.cloneNode(true)
    new_dom.addEventListener('click', function(){
        refreshPage(cur_dir)
    })
    dom.replaceWith(new_dom)  // clears old listeners
}

function _updateDOM(response_dict){
    // Update DOM
    refreshGallery(response_dict["file_keys"])
    _refreshInnerDirs(response_dict["inner_dirs"])
    _refreshOuterDirs(response_dict["outer_dir"])
}

async function refreshPage(cur_dir=""){
    var show_subdirs = document.querySelector("#show_subdirs").checked
    var show_hidden_content = document.querySelector("#show_hidden_content").checked

    var url = window.location.href + "refresh_page_params"
    var payload = {
        "cur_dir": cur_dir,
        "show_subdirs": show_subdirs,
        "show_hidden_content": show_hidden_content
    }

    var cur_dir_dom = document.querySelector("#cur_dir")
    cur_dir_dom.innerText = "cur_dir: /".concat(cur_dir)
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
        (data) => _updateDOM(data)
    )
}

async function copyKeys(){
    await fileAction("copy")
    var cur_dir = document.querySelector("#cur_dir").innerText.slice(10)
    refreshPage(cur_dir)
}

async function moveKeys(){
    await fileAction("move")
    var cur_dir = document.querySelector("#cur_dir").innerText.slice(10)
    refreshPage(cur_dir)
}

async function deleteKeys(){
    await fileAction("delete")
    var cur_dir = document.querySelector("#cur_dir").innerText.slice(10)
    refreshPage(cur_dir)
}

window.addEventListener('load', refreshPage())

const copy_action = document.getElementById("copy_action");
copy_action.addEventListener('click', copyKeys)

const move_action = document.getElementById("move_action");
move_action.addEventListener('click', moveKeys)

const delete_action = document.getElementById("delete_action");
delete_action.addEventListener('click', deleteKeys)
