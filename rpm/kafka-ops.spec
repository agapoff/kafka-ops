Name:       kafka-ops
Version:    %{version}
Release:    %{build_number}%{?dist}
Summary:    Tool for Kafka cluster resources management
Packager:   Vitaly Agapov <agapov.vitaly@gmail.com>
License:    Apache License, Version 2.0
BuildRoot:  %{_tmppath}/%{name}-%{version}-root

%description
Tool for Kafka cluster resources management

%prep

%build

%install
mkdir -p %{buildroot}/usr/bin
install -p -D -m 755 kafka-ops %{buildroot}/usr/bin

%files
%defattr(-,root,root)
/usr/bin/kafka-ops

%post

%changelog
* Fri Aug 16 2019 Vitaly Agapov <agapov.vitaly@gmail.com> - 1.0.0
- Initial release
