/*
 * jQuery File Upload Plugin JS Example 8.8.2
 * https://github.com/blueimp/jQuery-File-Upload
 *
 * Copyright 2010, Sebastian Tschan
 * https://blueimp.net
 *
 * Licensed under the MIT license:
 * http://www.opensource.org/licenses/MIT
 */

/*jslint nomen: true, regexp: true */
/*global $, window, blueimp */

'use strict';

let dominator = {

  init: function(DOMelement) {
    this.el = DOMelement;
    this.files = new Map();
  },

  setType: (file_id, type) => {
    if(dominator.files.has(file_id)) {
      dominator.files.get(file_id).type = type;
    }
  },

  add: (e, data) => {
    let element = $('select#' + data.files[0].name.split('.')[0]);
    if(!dominator.files.has(element[0].id)) {
      dominator.files.set(element[0].id,
        {
          type: ''
        }
      );
      element.on('change', (e) => {
        dominator.setType(element[0].id, e.currentTarget.value);
      });
    } else {
      data.abort(); // TODO make it abort add
    }
  },

}

$(function () {
    'use strict';

  dominator.init($('#fileupload')); // Initialize the dominator

    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
        // Uncomment the following to send cross-domain cookies:
        //xhrFields: {withCredentials: true},
        //url: 'server/php/'
      uploadTemplate: dominator.uploadTemplate
    }).on('fileuploadadded', dominator.add)
    .on('fileuploadsend', function(e, data) {
      data.data.set('file_type',
        dominator.files.get(data.data.get('file').name.split('.')[0]).type);
    })
    .on('fileuploadcompleted', function(e, data) {
      $('#fileupload_control .process').on('click', e => {
        console.log(data.result);
        let key = undefined;
        let file = data.result.files[0];
/*        if(file.name.split('_').length > 1) {*/
          //key = file.name.split('_')[1].split('.')[0];
        //} else if(file.name.split('_').length = 1) {
          //key = file.name.split('_')[0].split('.')[0];
        //}
        //let o = dominator.files.forEach(function(f) {
          //if(!f.hasOwnProperty('resource_id')
          //&& (dominator.files.has(key))) {
            //dominator.files.get(key).resource_id = .resource_id
          //}
        /*});*/
        $.get('/process/' + file.resource_id);
      });

      $('#fileupload_control .process').removeAttr('disabled');
    });

    // Enable iframe cross-domain access via redirect option:
    $('#fileupload').fileupload(
        'option',
        'redirect',
        window.location.href.replace(
            /\/[^\/]*$/,
            '/cors/result.html?%s'
        )
    );

    // Load existing files:
/*    $('#fileupload').addClass('fileupload-processing');*/
    //$.ajax({
        //// Uncomment the following to send cross-domain cookies:
        ////xhrFields: {withCredentials: true},
        ////url: $('#fileupload').fileupload('option', 'url'),
        //url: '/upload/view/',
        //dataType: 'json',
        //context: $('#fileupload')[0]
    //}).always(function () {
        //$(this).removeClass('fileupload-processing');
    //}).done(function (result) {
        //$(this).fileupload('option', 'done')
            //.call(this, null, {result: result});
    //});

});
