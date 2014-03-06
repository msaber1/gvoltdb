# This file is part of VoltDB.
# Copyright (C) 2008-2014 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

"""
voltpkg docker support module
"""

import os
import glob
import shutil
import shlex
from voltcli import utility
from voltcli import environment


class DockerTool(object):
    """
    Docker support class.

    Accesses the following configuration variables:
        base_image (required - only "ubuntu" is currently supported)
        base_tag (required) - image version,, e.g. 12.04 for ubuntu
        image_folder (required) - where to install it in the image
        voltdb_base (required) - base folder with README* and version.txt
        voltdb_bin (required) - bin folder with volt* scripts
        voltdb_lib (required) - lib folder with volt libraries
        voltdb_voltdb (required) - voltdb folder with volt jar
        repo_url (required for ubuntu) - OS repository URL
        repo_name (required for ubuntu) - OS repository name
        repo_sections (required for ubuntu) - OS repository sections
        dist_folder (defaults to "dist") - where distribution files are staged
        dockerfile_name (defaults to "Dockerfile")
        dockerfile_preamble (optional) - comment at top of docker file
        entrypoint (defaults to voltdb) - what to run
        workdir (defaults to image_folder) - where to run from
    """
    def __init__(self, runner, config):
        self.runner = runner
        self.config = config
        self.dockerfile_lines = []
        # Validate the configuration.
        if not 'base_image' in config:
            self.runner.abort('Configuration option "base_image" is required.')
        self.config['base_image'] = self.config['base_image'].lower()
        required = ['voltdb_base', 'image_folder', 'voltdb_bin', 'voltdb_lib', 'voltdb_voltdb']
        if self.config['base_image'] == 'ubuntu':
            required.extend(['repo_url', 'repo_name', 'repo_sections'])
        else:
            self.runner.abort('Unsupported base image "%s".' % self.config['base_image'])
        missing = [check for check in required if check not in self.config]
        if missing:
            self.runner.abort('The following required configuration options are not set.',
                              missing)
        # The default distribution folder is "dist".
        self.config['dist_folder'] = self.config.get('dist_folder', 'dist')
        # The default docker file is "Dockerfile".
        dockerfile_name = self.config.get('dockerfile_name', 'Dockerfile')
        self.config['dockerfile_path'] = dockerfile_name

    def generate(self):
        self.runner.info('Preparing distribution folder "%(dist_folder)s"...' % self.config)
        self._prepare_distribution_folder()
        self.runner.info('Preparing docker file "%(dockerfile_path)s"...' % self.config)
        self._prepare_dockerfile_lines()
        self._write_dockerfile()

    def _path(self, *parts):
        # Expand configuration variables in path parts.
        expanded_parts = [part % self.config for part in parts]
        return os.path.join(*expanded_parts)

    def _add_files(self, dir_in, dir_out, recursive, *patterns):
        if dir_out:
            root = self._path('%(dist_folder)s', dir_out)
        else:
            root = self._path('%(dist_folder)s')
        if not os.path.exists(root):
            self.runner.verbose_info('Creating distribution folder "%s"...' % root)
            os.makedirs(root)
        for pattern in patterns:
            glob_pattern = self._path(dir_in, pattern)
            self.runner.verbose_info('Looking for "%s"...' % glob_pattern)
            for path_in in sorted(glob.glob(glob_pattern)):
                if os.path.isdir(path_in) and recursive:
                    path_out = self._path(root, os.path.basename(path_in))
                    self.runner.verbose_info('Copying folder "%s" to "%s"...'
                                                    % (path_in, path_out))
                    shutil.copytree(path_in, path_out)
                else:
                    self.runner.verbose_info('Copying file "%s" to "%s"...'
                                                    % (path_in, root))
                    shutil.copy2(path_in, root)

    def _prepare_distribution_folder(self):
        try:
            dist_folder = self.config['dist_folder']
            voltdb_base = self.config['voltdb_base'],
            if os.path.exists(dist_folder):
                self.runner.verbose_info('Deleting existing distribution folder "%s"...' % dist_folder)
                shutil.rmtree(dist_folder)
            self.runner.info('Populating distribution folder "%s"...' % dist_folder)
            # Be selective when copying from the root folder of a source tree.
            self._add_files('%(voltdb_base)s', None, False, 'README*', 'version.txt')
            # Only bring in volt stuff from what could be a shared bin folder.
            self._add_files('%(voltdb_bin)s', 'bin', True, 'voltdb', 'voltadmin')
            # The lib and voldb folders should never be shared with anything else.
            self._add_files('%(voltdb_lib)s', 'lib', True, '*')
            self._add_files('%(voltdb_voltdb)s', 'voltdb', True, '*')
        except (IOError, OSError), e:
            self.runner.abort('Distribution folder preparation failed.', e)

    def _prepare_dockerfile_lines(self):
        self.dockerfile_lines = []
        self._add_line(self.config.get('dockerfile_preamble', None))
        self._add_line('FROM', '%(base_image)s:%(base_tag)s')
        self._add_line('ADD', '. %(image_folder)s/')
        # Pre-checked that it's a supported base image (just "ubuntu" for now).
        # Add else clauses for custom initialization of other base image types.
        if self.config['base_image'] == 'ubuntu':
            self._add_line('RUN', 'echo "deb %(repo_url)s %(repo_name)s %(repo_sections)s"',
                                '> /etc/apt/sources.list')
            self._add_line('RUN', 'apt-get update')
            self._add_line('RUN', 'apt-get install -y openjdk-7-jre-headless')
        # Generate WORKDIR statement by using the configured "workdir" or by
        # defaulting to the image folder.
        workdir = self.config.get('workdir', None)
        if not workdir:
            workdir = '%(image_folder)s'
        self._add_line('WORKDIR', workdir)
        # Generate Dockerfile ENTRYPOINT by splitting command line arguments
        # and wrapping as a list of quoted strings inside square brackets.
        # Treat the relative paths as relative to the target folder inside the image.
        if 'entrypoint' in self.config:
            args = shlex.split(self.config['entrypoint'])
            if not args[0][0] in ['/', '.']:
                args[0] = self._path('%(image_folder)s', args[0])
        else:
            args = [self._path('%(image_folder)s', '%(dist_folder)s', 'bin', 'voltdb')]
        self._add_line('ENTRYPOINT', '[%s]' % '.'.join(['"%s"' % arg for arg in args]))
        # TODO: Deal with ports?
        #self._add_line('EXPOSE', '21212')

    def _write_dockerfile(self):
        try:
            f = utility.File(self.config['dockerfile_path'], mode='w')
            f.open()
            for line in self.dockerfile_lines:
                f.write('%s\n' % line)
        finally:
            f.close()

    def _add_line(self, *args):
        args_out = []
        for arg in args:
            if arg:
                if arg.find('%(') != -1:
                    args_out.append(arg % self.config)
                else:
                    args_out.append(arg)
        if args_out:
            self.dockerfile_lines.append(' '.join(args_out))
