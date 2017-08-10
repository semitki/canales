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
    this.files = new Map();   // Metadata related to uploaded files
    this.queueFiles = 0;      // Files in the process queue
    this.processedFiles = 0;  // Number of succesfully processed files
  },

  setType: (file_id, type) => {
    if(dominator.files.has(file_id)) {
      dominator.files.get(file_id).type = type;
    }
  },

  /**
   * Callback function triggered when files have been added to upload queue
   */
  add: (e, data) => {
    let element = $('select#' + data.files[0].name.split('.')[0]);
    // if(!dominator.files.has(element[0].id)) {
    if(dominator.queueFiles < 2) {
      dominator.files.set(element[0].id,
        {
          type: ''
        }
      );
      element.on('change', (e) => {
        dominator.setType(element[0].id, e.currentTarget.value);
      });
      dominator.queueFiles++;
    } else {
      data.abort(); // TODO make it abort add
    }
  },

  /**
   * Callback function to send uploaded files to processing in sqlizer
   */
  processFiles: data => {
    let key = undefined;
    let file = data.result.files[0];
    $.get('/process/' + file.resource_id,
      {},
      function(response) {
        if(response.Status === 'Complete') {
          if(dominator.processedFiles < 2) {
            dominator.files.get(response.FileName.split('.')[0]).data = response
            dominator.processedFiles++;
          }
          if(dominator.processedFiles == 2) {
            let data = {};
            dominator.files.forEach((k, v) => {
              data[v] = k;
            });
            console.log(data);
            dominator.processed(data);
          }
        }
    });
  },

  processed: data => {
    console.log(data);
    $.get('/postproc/',
      data,
      function(reponse) {
        console.log(response);
    });
    console.log('ya terminaron los 2 archivos haz algo aqui');
  }

}

$(function () {
  'use strict';

  dominator.init($('#fileupload')); // Initialize the dominator

    // Initialize the jQuery File Upload widget:
    $('#fileupload').fileupload({
      uploadTemplate: dominator.uploadTemplate
    }).on('fileuploadadded', dominator.add)
    .on('fileuploadsend', function(e, data) {
      // TODO make file type select elements mutally exclusive
      data.data.set('file_type',
        dominator.files.get(data.data.get('file').name.split('.')[0]).type);
    })
    .on('fileuploadcompleted', function(e, data) {
      $('#fileupload_control .process').on('click',
        dominator.processFiles(data));
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
});
