
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

python::virtualenv { '/vagrant/hypothesis_graph':
    ensure => present,
    version => 'system',
    requirements => '/var/www/hypothesis_graph_www/requirements.txt',
    systempkgs => true,
    distribute => false,
    owner => 'root',
    group => 'admin',
    cwd => '/var/www/hypothesis_graph_www',
    timeout => 1800,
}

# Postgresql

class { 'postgresql::globals':
    version => '9.3',
    manage_package_repo => true,
    encoding => 'UTF8',
    locale => 'en_US.utf8',
}->
class { 'postgresql::server':
    ensure => 'present',
    listen_addresses => '*',
    manage_firewall => true,
}

class { 'postgresql::server::contrib':
    package_ensure => 'present',
}

exec { 'apt-get update':
    command => '/usr/bin/apt-get update',
    path => 'bin',
}

file { '/var/www/hypothesis_graph_www':
    ensure => 'link',
    force => true,
    target => '/vagrant',
}
