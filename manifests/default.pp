
# Nginx

class { 'nginx': }

# Python

class { 'python':
    version => '2.7',
    pip => true,
    dev => true,
    virtualenv => true,
    gunicorn => false,
}

python::virtualenv { '/vagrant':
    ensure => present,
    version => 'system',
    requirements => '/vagrant/requirements.txt',
    systempkgs => true,
    distribute => false,
    owner => 'root',
    group => 'admin',
    cwd => '/vagrant',
    timeout => 1800,
}

# Postgresql

package { ['postgresql-server-dev-9.3']:
  ensure  => 'installed',
  before  => Class['postgresql::server']
}

class { 'postgresql::globals':
    version => '9.3',
    encoding => 'UTF8',
    locale => 'en_US.utf8',
    server_package_name => 'postgresql-9.3',
    contrib_package_name => 'postgresql-contrib-9.3',
}->
class { 'postgresql::server':
    package_ensure => 'installed',
    listen_addresses => '*',
    manage_firewall => true,
}

# XXX SET NEW VALUE WHEN THIS GOES ONLINE
postgresql::server::role { 'hg':
  password_hash => postgresql_password('hg', 'hgtest'),
  superuser => true,
}

# System packages

exec { 'apt-get update':
    command => '/usr/bin/apt-get update',
    path => 'bin',
}

$lxml_reqs = ['libxml2-dev', 'libxslt1-dev']
package { $lxml_reqs: ensure => 'latest' }

package { 'iptables-persistent': 
    ensure => 'latest',
}

file { '/var/www/hypothesis_graph_www':
    ensure => 'link',
    force => true,
    target => '/vagrant',
}
