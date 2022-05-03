import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { Posts4Component } from './posts4.component';

describe('Posts4Component', () => {
  let component: Posts4Component;
  let fixture: ComponentFixture<Posts4Component>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ Posts4Component ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(Posts4Component);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
