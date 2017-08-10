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
    this.processed = 0;
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

  processed: data => {
    console.log('ya terminaron los 2 archivos haz algo aqui');
  }

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
        let key = undefined;
        let file = data.result.files[0];
        $.get('/process/' + file.resource_id,
          {},
          function(response) {
            if(response.Status === 'Complete') {
              console.log(dominator.processed)
              if(dominator.processed < 2) {
                dominator.processed++;
              }
              if(dominator.processed == 2) {
                console.log('terminados ambos 2');
                dominator.processed({});
              }
            }
        });
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
});
