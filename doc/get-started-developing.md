# Running Metadb/FOLIO on a new Linux box


## Prerequisites


### VirtualBox

This is the software that provides the virtual machine that FOLIO will run in. It seems not to be included as part of the Debian stable, so must be manually installed from Debian package provided by the vendor.

Download the most recent version of VIrtualBox from https://www.virtualbox.org/wiki/Linux_Downloads and use `sudo dpkg -i virtualbox-7.0_7.0.18-162988~Debian~bookworm_amd64.deb` or similar to install the downloaded package.

This will typically result in a list of prerequisite packages that are not installed: for example libqt5xml5. If this happens, `sudo apt -f install` will pull in the missing package and complete the installation of VirtualBox.

When running `virtualbox` for the firt time, the terminal where it was launched may complain:
> WARNING: The vboxdrv kernel module is not loaded. Either there is no module available for the current kernel (6.1.0-21-amd64) or it failed to load.

If this happens, `sudo /sbin/vboxconfig` should solve the problem.

### Other prerequisites

Vagrant is used to manage the VirtualBox VMs, and the Go language compiles Metadb.

```
sudo apt-get install vagrant
sudo apt-get unstall golang
```


## FOLIO

Before you can run Metadb in a meaningful way, you need a FOLIO system set up to feed it events. It's politically difficult to get event feeds from existing FOLIO installations, and probaby impossible to get permission to set up your own feed -- which you want to do if you're going to get to grips with the whole Metadb system. So you want your own FOLIO system that you can do with as you please.

The simplest way to get a running FOLIO system is as a pre-packaged vagrant box. The process is described in detail in [Create workspace and launch the VM](https://dev.folio.org/tutorials/folio-vm/01-create-workspace/), but in a nutshell:
```
host$ mkdir folio-release
cd folio-release
cat > Vagrantfile << 'EOF'
Vagrant.configure("2") do |config|
  config.vm.box = "folio/release"
  config.vm.provider "virtualbox" do |vb|
    vb.memory = 24576
    vb.cpus = 2
  end
end
EOF
host$ vagrant up
```
If `vagrant up` fails with "Call to virConnectOpen failed: Failed to connect socket to '/var/run/libvirt/libvirt-sock': No such file or directory", that most likely means that you forgot to install Vagrant: see above.

Once the VM is running, you can enter it and check that it's running Okapi by asking it for its version number:
```
host$ vagrant ssh
vagrant@vagrant:~$ curl -w '\n' localhost:9130/_/version
5.1.2
vagrant@vagrant:~$ exit
```
Then check that Okapi is being correctly tunnelled out to the host system:
```
host$ curl -w '\n' localhost:9130/_/version
5.1.2
```
Congratulations, you now have a FOLIO system running in a virtual machine and accessible from the host. You could now, if you wished, run Stripes against this FOLIO backend.

